package de.dataflair.netty;

import de.dataflair.netty.client.NettyClient;
import de.dataflair.netty.common.codec.PacketRegistry;
import de.dataflair.netty.common.packet.Packet;
import de.dataflair.netty.common.packet.auth.AuthPacket;
import de.dataflair.netty.common.packet.json.JsonPacket;
import de.dataflair.netty.server.NettyServer;
import de.dataflair.netty.testpackets.TestPacket;
import de.dataflair.netty.testpackets.TestRequestPacket;
import de.dataflair.netty.testpackets.TestRespondPacket;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.ResourceLeakDetector;
import io.netty.util.ResourceLeakDetectorFactory;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * With {@link ResourceLeakDetector.Level#PARANOID} every tracked buffer is checked; the codec and a real
 * client-server exchange must not leak. The test runs in its own JVM (forkEvery = 1) so the recording detector
 * factory is installed before Netty creates its leak detectors.
 */
class LeakTest {
    private static final List<String> LEAKS = new CopyOnWriteArrayList<>();

    static {
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
        ResourceLeakDetectorFactory.setResourceLeakDetectorFactory(new ResourceLeakDetectorFactory() {
            @Override
            @SuppressWarnings("deprecation")
            public <T> ResourceLeakDetector<T> newResourceLeakDetector(Class<T> resource, int samplingInterval, long maxActive) {
                return new ResourceLeakDetector<>(resource, samplingInterval) {
                    @Override
                    protected void reportTracedLeak(String resourceType, String records) {
                        LEAKS.add(resourceType + records);
                    }

                    @Override
                    protected void reportUntracedLeak(String resourceType) {
                        LEAKS.add(resourceType);
                    }
                };
            }
        });
    }

    /**
     * Runs GC and allocates tracked buffers so the detector drains its reference queue.
     */
    private static void collectLeaks() throws InterruptedException {
        for (int i = 0; i < 10; i++) {
            System.gc();
            Thread.sleep(50);
            for (int j = 0; j < 100; j++) {
                ByteBufAllocator.DEFAULT.buffer(16).release();
                ByteBufAllocator.DEFAULT.heapBuffer(16).release();
            }
        }
    }

    @SuppressWarnings("unused")
    private static void leakOneBuffer() {
        ByteBuf leaked = ByteBufAllocator.DEFAULT.buffer(32);
        leaked.writeInt(1);
    }

    private static EmbeddedChannel codecChannel(boolean requireAuthentication) {
        return new EmbeddedChannel(new NettyChannelInitializer(null, 1024, new PacketRegistry().allowAnyPacketClass(), requireAuthentication) {
            @Override
            public SimpleChannelInboundHandler<?> handler() {
                return new SimpleChannelInboundHandler<Packet>() {
                    @Override
                    protected void channelRead0(ChannelHandlerContext ctx, Packet msg) {
                        ctx.fireChannelRead(msg);
                    }
                };
            }
        });
    }

    private static void roundTrip(EmbeddedChannel channel, Packet packet) {
        channel.writeOutbound(packet);
        ByteBuf out;
        while ((out = channel.readOutbound()) != null) {
            channel.writeInbound(out);
        }
        assertNotNull(channel.readInbound());
    }

    private static ByteBuf frame(String className, int contentLength) {
        var name = className.getBytes(StandardCharsets.UTF_8);
        var buf = ByteBufAllocator.DEFAULT.buffer();
        buf.writeInt(Integer.BYTES + name.length + Integer.BYTES + contentLength);
        buf.writeInt(name.length).writeBytes(name).writeInt(contentLength).writeZero(contentLength);
        return buf;
    }

    private static void codecWorkload() {
        var channel = codecChannel(false);
        for (int i = 0; i < 500; i++) {
            roundTrip(channel, new TestPacket("m" + i, i));
            roundTrip(channel, new JsonPacket("{\"i\":" + i + "}"));
            var request = new TestRequestPacket("t" + i, List.of("x"));
            var id = UUID.randomUUID();
            request.queryId(id);
            request.buffer().writeUniqueId(id);
            request.writeBuffer(request.buffer());
            roundTrip(channel, request);
            var respond = new TestRespondPacket("r" + i);
            respond.queryId(id);
            respond.buffer().writeUniqueId(id);
            respond.writeBuffer(respond.buffer());
            roundTrip(channel, respond);
        }
        // rejected frames: unknown class, not a packet, too long
        for (int i = 0; i < 100; i++) {
            assertThrows(Exception.class, () -> channel.writeInbound(frame("com.example.Missing", 4)));
            assertThrows(Exception.class, () -> channel.writeInbound(frame("java.lang.String", 4)));
            assertThrows(Exception.class, () -> channel.writeInbound(frame(TestPacket.class.getName(), 4096)));
        }
        assertFalse(channel.finishAndReleaseAll());

        // rejected before authentication
        for (int i = 0; i < 100; i++) {
            var unauthenticated = codecChannel(true);
            assertThrows(Exception.class, () -> unauthenticated.writeInbound(frame(TestPacket.class.getName(), 8)));
            unauthenticated.finishAndReleaseAll();
        }
    }

    private static void networkWorkload() throws Exception {
        var server = new NettyServer("127.0.0.1", 0);
        server.allowAnonymous(true);
        server.packetRegistry(new PacketRegistry().allowAnyPacketClass());
        server.initialize();
        server.connectionFuture().get(5, TimeUnit.SECONDS);
        var client = new NettyClient("127.0.0.1", server.localAddress().getPort(),
                new NettyClientChannel.Identity("leak", UUID.randomUUID()), Map.of());
        client.packetRegistry(new PacketRegistry().allowAnyPacketClass());
        try {
            server.packetTransmitter().listen(TestPacket.class, (sender, packet) -> sender.sendPacket(packet));
            server.packetTransmitter().listenQuery(TestRequestPacket.class, r -> new TestRespondPacket(r.tag()));
            int count = 200;
            var echoes = new CountDownLatch(count);
            client.packetTransmitter().listen(TestPacket.class, (c, p) -> echoes.countDown());
            client.initialize();
            client.connectionFuture().get(5, TimeUnit.SECONDS);
            for (int i = 0; i < count; i++) {
                client.thisChannel().sendPacket(new TestPacket("e" + i, i));
            }
            assertTrue(echoes.await(10, TimeUnit.SECONDS));
            for (int i = 0; i < 50; i++) {
                TestRespondPacket response = client.packetTransmitter()
                        .queryPacketDirect(new TestRequestPacket("q" + i, List.of()), TestRespondPacket.class);
                assertNotNull(response);
            }
            client.thisChannel().sendPacket(new AuthPacket(client.identity(), Map.of()));
            var last = new CompletableFuture<Void>();
            client.packetTransmitter().listen(JsonPacket.class, (c, p) -> last.complete(null));
            server.connections().getFirst().sendPacket(new JsonPacket("{}"));
            last.get(5, TimeUnit.SECONDS);
        } finally {
            client.shutdownGracefully(0, 1, TimeUnit.SECONDS).await(5, TimeUnit.SECONDS);
            server.shutdownGracefully(0, 1, TimeUnit.SECONDS).await(5, TimeUnit.SECONDS);
            server.workerGroup().terminationFuture().await(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void codecAndNetworkDoNotLeak() throws Exception {
        assertEquals(ResourceLeakDetector.Level.PARANOID, ResourceLeakDetector.getLevel());

        // positive control: the detector must report a deliberately leaked buffer
        leakOneBuffer();
        long deadline = System.currentTimeMillis() + 10_000;
        while (LEAKS.isEmpty() && System.currentTimeMillis() < deadline) {
            collectLeaks();
        }
        assertFalse(LEAKS.isEmpty(), "leak detector did not report the deliberate leak");
        LEAKS.clear();

        codecWorkload();
        networkWorkload();
        collectLeaks();
        collectLeaks();
        assertEquals(List.of(), LEAKS, "buffers leaked");
    }
}

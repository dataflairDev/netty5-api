package de.dataflair.netty5.attack;

import de.dataflair.netty5.Netty5ClientChannel;
import de.dataflair.netty5.Netty5Component;
import de.dataflair.netty5.Netty5Identities;
import de.dataflair.netty5.actions.ConnectionAction;
import de.dataflair.netty5.client.Netty5Client;
import de.dataflair.netty5.common.codec.PacketRegistry;
import de.dataflair.netty5.testpackets.TestPacket;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static de.dataflair.netty5.attack.SecurityHardeningTest.closedWithin;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Keep-alive against the reader idle timeout, bidirectional backpressure, {@code isWritable()} and the
 * configurable rejected-packet limit of the client.
 */
class KeepAliveAndBackpressureTest {

    // ---- M4 keep-alive ------------------------------------------------------------------

    @Test
    void quietClientWithKeepAliveStaysConnected() throws Exception {
        var disconnects = new AtomicInteger();
        var harness = AttackHarness.start(false, 10_000, server -> {
            server.idleTimeoutMillis(600);
            server.addAction(new ConnectionAction(_ -> disconnects.incrementAndGet(),
                    ConnectionAction.State.CLIENT_DISCONNECTED));
        });
        var client = client(harness, "quiet");
        try {
            client.keepAliveIntervalMillis(150);
            client.initialize();
            var effective = client.authenticationFuture().get(5, TimeUnit.SECONDS);
            assertEquals(Netty5Identities.derivedUuid("quiet"), effective.uuid());
            assertEquals(effective.uuid(), client.thisChannel().identity().uuid());
            var serverSide = harness.awaitAuthenticated("quiet", 5_000);

            Thread.sleep(2_500); // more than four idle timeouts without any application packet
            assertEquals(0, disconnects.get(), "keep-alives must keep the connection open");
            assertTrue(serverSide.channel().isActive());
            assertSame(serverSide, harness.server().firstChannel("quiet"));
            assertEquals(Netty5Component.ConnectionState.CONNECTED, client.connectionState());
        } finally {
            stop(client);
            harness.stop();
        }
    }

    @Test
    void quietClientWithoutKeepAliveIsClosed() throws Exception {
        var harness = AttackHarness.start(false, 10_000, server -> server.idleTimeoutMillis(500));
        var client = client(harness, "mute");
        try {
            client.keepAliveIntervalMillis(0);
            client.initialize();
            var serverSide = harness.awaitAuthenticated("mute", 5_000);
            assertTrue(serverSide.channel().closeFuture().await(3, TimeUnit.SECONDS),
                    "without keep-alives the idle timeout closes the connection");
        } finally {
            stop(client);
            harness.stop();
        }
    }

    @Test
    void mutePeerIsClosedAlthoughTheServerKeepsWriting() throws Exception {
        var harness = AttackHarness.start(false, 10_000, server -> server.idleTimeoutMillis(500));
        try (var socket = harness.connect()) {
            AttackHarness.send(socket, AttackHarness.authFrame("half-open", AttackHarness.TOKEN));
            var channel = harness.awaitAuthenticated("half-open", 5_000);
            // the server writes every 100 ms; only reading resets the reader idle timeout
            var writer = channel.channel().eventLoop().scheduleAtFixedRate(
                    () -> channel.sendPacket(new TestPacket("tick", 1)), 0, 100, TimeUnit.MILLISECONDS);
            try {
                assertTrue(channel.channel().closeFuture().await(3, TimeUnit.SECONDS),
                        "a peer that sends nothing is closed even while the server writes");
            } finally {
                writer.cancel(false);
            }
            assertTrue(closedWithin(socket, 2_000));
        } finally {
            harness.stop();
        }
    }

    // ---- M1 bidirectional backpressure, L2 isWritable --------------------------------------

    @Test
    void bothSidesSendingALotDoNotDeadlock() throws Exception {
        var harness = AttackHarness.start(true, 10_000, _ -> {
        });
        var client = client(harness, "flood");
        var received = new AtomicInteger();
        client.packetTransmitter().listen(TestPacket.class, (_, _) -> received.incrementAndGet());
        try {
            assertFalse(client.isWritable(), "not connected yet");
            client.initialize();
            client.authenticationFuture().get(5, TimeUnit.SECONDS);
            assertTrue(client.isWritable());

            var payload = "x".repeat(4_000);
            var burst = 1_500; // about 6 MB per burst: both sides are far above their high water marks at once
            var bursts = 2;
            var deadline = System.currentTimeMillis() + 30_000;
            for (int b = 0; b < bursts; b++) {
                for (int i = 0; i < burst; i++) {
                    client.thisChannel().sendPacket(new TestPacket(payload, i));
                }
                assertFalse(client.isWritable(), "the burst exceeds the high water mark");
                // a sender waits for isWritable(); before the fix both peers stopped reading and this never ended
                while (!client.isWritable() || received.get() < (b + 1) * burst) {
                    if (System.currentTimeMillis() > deadline) {
                        throw new AssertionError("hang: writable=" + client.isWritable() + ", "
                                + received.get() + " of " + (b + 1) * burst + " echoes");
                    }
                    Thread.sleep(5);
                }
            }
            assertEquals(bursts * burst, received.get(), "every echo arrives");
            assertTrue(client.thisChannel().channel().isActive());
        } finally {
            stop(client);
            harness.stop();
        }
    }

    // ---- L5 client limit for rejected packets ----------------------------------------------

    @Test
    void clientRejectedPacketLimitIsConfigurable() throws Exception {
        var harness = AttackHarness.start();
        // this client does not register TestPacket, so every TestPacket of the server is rejected
        var client = new Netty5Client("127.0.0.1", harness.port(),
                new Netty5ClientChannel.Identity("strict", UUID.randomUUID()), Map.of("token", AttackHarness.TOKEN));
        client.packetRegistry(new PacketRegistry());
        client.reconnectDelayMillis(60_000);
        client.maxRejectedPackets(2);
        try {
            client.initialize();
            var serverSide = harness.awaitAuthenticated("strict", 5_000);
            serverSide.sendPacket(new TestPacket("one", 1));
            Thread.sleep(300);
            assertTrue(serverSide.channel().isActive(), "one rejected packet is below the limit");
            serverSide.sendPacket(new TestPacket("two", 2));
            assertTrue(serverSide.channel().closeFuture().await(3, TimeUnit.SECONDS),
                    "the client closes after two rejected packets");
        } finally {
            stop(client);
            harness.stop();
        }
    }

    private static Netty5Client client(AttackHarness harness, String name) {
        var client = new Netty5Client("127.0.0.1", harness.port(),
                new Netty5ClientChannel.Identity(name, UUID.randomUUID()), Map.of("token", AttackHarness.TOKEN));
        client.packetRegistry(new PacketRegistry().register(TestPacket.class));
        client.reconnectDelayMillis(60_000);
        return client;
    }

    private static void stop(Netty5Client client) throws InterruptedException {
        client.shutdownGracefully(0, 1, TimeUnit.SECONDS).await(5, TimeUnit.SECONDS);
    }
}

package de.dataflair.netty5.common.codec;

import de.dataflair.netty5.Netty5ChannelInitializer;
import de.dataflair.netty5.Netty5ChannelUtils;
import de.dataflair.netty5.Netty5ClientChannel;
import de.dataflair.netty5.common.packet.Packet;
import de.dataflair.netty5.common.packet.auth.AuthPacket;
import de.dataflair.netty5.common.packet.json.JsonPacket;
import de.dataflair.netty5.common.packet.json.JsonRequestPacket;
import de.dataflair.netty5.common.packet.json.JsonRespondPacket;
import de.dataflair.netty5.server.Netty5Server;
import de.dataflair.netty5.testpackets.ProbeFlags;
import de.dataflair.netty5.testpackets.TestPacket;
import de.dataflair.netty5.testpackets.TestQueryPacket;
import de.dataflair.netty5.testpackets.TestRequestPacket;
import de.dataflair.netty5.testpackets.TestRespondPacket;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.EncoderException;
import io.netty.handler.codec.TooLongFrameException;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class PacketCodecTest {

    static EmbeddedChannel channel(int maxFrameLength, PacketRegistry registry, boolean requireAuthentication) {
        return new EmbeddedChannel(new Netty5ChannelInitializer(null, maxFrameLength, registry, requireAuthentication) {
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

    static EmbeddedChannel channel() {
        return channel(Netty5ChannelUtils.DEFAULT_MAX_FRAME_LENGTH, new PacketRegistry(), false);
    }

    /**
     * Encodes the packet on the outbound side and feeds all produced bytes into the inbound side.
     */
    @SuppressWarnings("unchecked")
    static <P extends Packet> P roundTrip(EmbeddedChannel channel, Packet packet) {
        assertTrue(channel.writeOutbound(packet));
        ByteBuf out;
        while ((out = channel.readOutbound()) != null) {
            channel.writeInbound(out);
        }
        return (P) channel.readInbound();
    }

    /**
     * Builds a complete frame: frame length, class name, content length, content.
     */
    static ByteBuf frame(String className, byte[] content) {
        var name = className.getBytes(StandardCharsets.UTF_8);
        var buf = ByteBufAllocator.DEFAULT.buffer();
        buf.writeInt(Integer.BYTES + name.length + Integer.BYTES + content.length);
        buf.writeInt(name.length).writeBytes(name).writeInt(content.length).writeBytes(content);
        return buf;
    }

    @Test
    void plainPacketRoundTrip() {
        var channel = channel();
        TestPacket decoded = roundTrip(channel, new TestPacket("hello", 42));
        assertEquals("hello", decoded.message());
        assertEquals(42, decoded.number());
        assertFalse(channel.finish());
    }

    @Test
    void samePacketCanBeSentTwice() {
        var channel = channel();
        var packet = new TestPacket("again", 7);
        TestPacket first = roundTrip(channel, packet);
        TestPacket second = roundTrip(channel, packet);
        assertEquals("again", first.message());
        assertEquals("again", second.message());
        assertEquals(7, second.number());
        channel.finishAndReleaseAll();
    }

    @Test
    void builtInPacketsRoundTrip() {
        var channel = channel();
        var identity = new Netty5ClientChannel.Identity("client", UUID.randomUUID());
        AuthPacket auth = roundTrip(channel, new AuthPacket(identity, Map.of("token", "secret")));
        assertEquals("client", auth.identity().name());
        assertEquals(identity.uuid(), auth.identity().uuid());
        assertEquals(Map.of("token", "secret"), auth.properties());

        JsonPacket json = roundTrip(channel, new JsonPacket("{\"a\":1}"));
        assertEquals("{\"a\":1}", json.json());
        JsonPacket nullJson = roundTrip(channel, new JsonPacket((String) null));
        assertNull(nullJson.json());
        channel.finishAndReleaseAll();
    }

    /**
     * Fix 3 and 19: request, respond and query packets keep query id and payload, field initialisers of the
     * subclass do not overwrite decoded values.
     */
    @Test
    void requestRespondQueryRoundTrip() {
        var channel = channel();
        var queryId = UUID.randomUUID();

        var request = new TestRequestPacket("tag", List.of("a", "b"));
        request.queryId(queryId);
        request.buffer().writeUniqueId(queryId);
        request.writeBuffer(request.buffer());
        TestRequestPacket decodedRequest = roundTrip(channel, request);
        assertEquals(queryId, decodedRequest.queryId());
        assertEquals("tag", decodedRequest.tag());
        assertEquals(List.of("a", "b"), decodedRequest.items());

        var respond = new TestRespondPacket("value");
        respond.queryId(queryId);
        respond.buffer().writeUniqueId(queryId);
        respond.writeBuffer(respond.buffer());
        TestRespondPacket decodedRespond = roundTrip(channel, respond);
        assertEquals(queryId, decodedRespond.queryId());
        assertEquals("value", decodedRespond.value());

        var query = new TestQueryPacket("text");
        query.queryId(queryId);
        query.buffer().writeUniqueId(queryId);
        query.writeBuffer(query.buffer());
        TestQueryPacket decodedQuery = roundTrip(channel, query);
        assertEquals(queryId, decodedQuery.queryId());
        assertEquals("text", decodedQuery.text());

        var jsonRequest = new JsonRequestPacket("{}");
        jsonRequest.queryId(queryId);
        jsonRequest.buffer().writeUniqueId(queryId);
        jsonRequest.writeBuffer(jsonRequest.buffer());
        JsonRequestPacket decodedJsonRequest = roundTrip(channel, jsonRequest);
        assertEquals("{}", decodedJsonRequest.json());

        var jsonRespond = new JsonRespondPacket("[]");
        jsonRespond.queryId(queryId);
        jsonRespond.buffer().writeUniqueId(queryId);
        jsonRespond.writeBuffer(jsonRespond.buffer());
        JsonRespondPacket decodedJsonRespond = roundTrip(channel, jsonRespond);
        assertEquals("[]", decodedJsonRespond.json());
        assertEquals(queryId, decodedJsonRespond.queryId());
        channel.finishAndReleaseAll();
    }

    /**
     * Fix 4: unknown and non-packet classes are rejected, the non-packet class is not initialised.
     */
    @Test
    void unknownAndNonPacketClassesAreRejectedWithoutInitialisation() {
        var channel = channel();
        assertThrows(PacketRejectedException.class,
                () -> channel.writeInbound(frame("com.example.DoesNotExist", new byte[0])));
        assertThrows(PacketRejectedException.class,
                () -> channel.writeInbound(frame("java.lang.String", new byte[0])));
        assertThrows(PacketRejectedException.class,
                () -> channel.writeInbound(frame("de.dataflair.netty5.testpackets.NotAPacket", new byte[0])));
        assertThrows(PacketRejectedException.class,
                () -> channel.writeInbound(frame("de.dataflair.netty5.testpackets.AbstractProbePacket", new byte[0])));
        assertNull(channel.readInbound());
        assertFalse(ProbeFlags.NOT_A_PACKET_INITIALIZED.get(), "NotAPacket must not be initialised");
        assertFalse(ProbeFlags.ABSTRACT_PACKET_INITIALIZED.get(), "abstract packet must not be initialised");
        channel.finishAndReleaseAll();
    }

    /**
     * Fix 4: before authentication only the auth packet is accepted; other classes are not even loaded.
     */
    @Test
    void onlyAuthPacketBeforeAuthentication() {
        var channel = channel(Netty5ChannelUtils.DEFAULT_MAX_FRAME_LENGTH, new PacketRegistry(), true);
        assertThrows(PacketRejectedException.class,
                () -> channel.writeInbound(frame("de.dataflair.netty5.testpackets.PreAuthProbePacket", new byte[0])));
        assertFalse(ProbeFlags.PRE_AUTH_PACKET_INITIALIZED.get(), "packet before auth must not be initialised");
        assertFalse(channel.isOpen(), "channel must be closed");
        channel.finishAndReleaseAll();

        var second = channel(Netty5ChannelUtils.DEFAULT_MAX_FRAME_LENGTH, new PacketRegistry(), true);
        AuthPacket auth = roundTrip(second, new AuthPacket(new Netty5ClientChannel.Identity("c", UUID.randomUUID()), Map.of()));
        assertEquals("c", auth.identity().name());
        second.attr(Netty5ChannelUtils.AUTHENTICATED).set(Boolean.TRUE);
        TestPacket afterAuth = roundTrip(second, new TestPacket("ok", 1));
        assertEquals("ok", afterAuth.message());
        second.finishAndReleaseAll();
    }

    /**
     * Fix 4: allowlist mode only accepts registered packets.
     */
    @Test
    void restrictedRegistryOnlyAcceptsRegisteredPackets() {
        var registry = new PacketRegistry().restrictToRegistered(true);
        var channel = channel(Netty5ChannelUtils.DEFAULT_MAX_FRAME_LENGTH, registry, false);
        assertThrows(PacketRejectedException.class,
                () -> channel.writeInbound(frame("de.dataflair.netty5.testpackets.RestrictedProbePacket", new byte[0])));
        assertFalse(ProbeFlags.RESTRICTED_PACKET_INITIALIZED.get());
        assertThrows(PacketRejectedException.class, () -> roundTrip(channel, new TestPacket("x", 1)));

        registry.register(TestPacket.class);
        TestPacket accepted = roundTrip(channel, new TestPacket("y", 2));
        assertEquals("y", accepted.message());
        JsonPacket builtIn = roundTrip(channel, new JsonPacket("{}"));
        assertEquals("{}", builtIn.json());
        assertThrows(IllegalArgumentException.class,
                () -> registry.register(de.dataflair.netty5.testpackets.AbstractProbePacket.class));
        channel.finishAndReleaseAll();
    }

    /**
     * Fix 5: frames larger than maxFrameLength are rejected; default is 16 MiB.
     */
    @Test
    void tooLargeFrameIsRejected() {
        assertEquals(16 * 1024 * 1024, Netty5ChannelUtils.DEFAULT_MAX_FRAME_LENGTH);
        assertEquals(16 * 1024 * 1024, new Netty5Server("127.0.0.1", 0).maxFrameLength());

        var channel = channel(64, new PacketRegistry(), false);
        assertThrows(TooLongFrameException.class,
                () -> channel.writeInbound(frame(TestPacket.class.getName(), new byte[200])));
        assertNull(channel.readInbound());
        TestPacket small = roundTrip(channel, new TestPacket("s", 1));
        assertEquals("s", small.message());
        channel.finishAndReleaseAll();
    }

    @Test
    void invalidLengthsAreRejected() {
        var channel = channel();
        var buf = ByteBufAllocator.DEFAULT.buffer();
        buf.writeInt(8).writeInt(-5).writeInt(0);
        assertThrows(PacketRejectedException.class, () -> channel.writeInbound(buf));

        var name = TestPacket.class.getName().getBytes(StandardCharsets.UTF_8);
        var buf2 = ByteBufAllocator.DEFAULT.buffer();
        buf2.writeInt(Integer.BYTES + name.length + Integer.BYTES).writeInt(name.length).writeBytes(name).writeInt(1000);
        assertThrows(PacketRejectedException.class, () -> channel.writeInbound(buf2));
        channel.finishAndReleaseAll();
    }

    /**
     * Fix 22: a failing encode writes no partial frame.
     */
    @Test
    void failingEncodeWritesNothing() {
        var channel = channel();
        var broken = new TestPacket("x", 1);
        broken.buffer(null);
        assertThrows(EncoderException.class, () -> channel.writeOutbound(broken));
        assertNull(channel.readOutbound());
        channel.finishAndReleaseAll();
    }
}

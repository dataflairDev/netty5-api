package de.dataflair.netty5.common.codec;

import de.dataflair.netty5.Netty5ChannelInitializer;
import de.dataflair.netty5.Netty5ChannelUtils;
import de.dataflair.netty5.common.log.SafeLog;
import de.dataflair.netty5.common.packet.Packet;
import de.dataflair.netty5.testpackets.DeepRecursionPacket;
import de.dataflair.netty5.testpackets.ExplodingInitPacket;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.DecoderException;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Deserialisation limits of {@link CodecBuffer}, fatal errors in the decoder and log sanitising.
 */
class HardeningCodecTest {

    private enum Color {RED, GREEN}

    private static CodecBuffer buffer() {
        return new CodecBuffer(Unpooled.buffer());
    }

    @Test
    void listSizeIsCheckedAgainstReadableBytes() {
        var buffer = buffer().writeInt(Integer.MAX_VALUE).writeByte((byte) 1);
        assertThrows(IllegalArgumentException.class, () -> buffer.readByteList(new ArrayList<>()));
        var negative = buffer().writeInt(-1);
        assertThrows(IllegalArgumentException.class, () -> negative.readByteList(new ArrayList<>()));
    }

    @Test
    void listMaxSizeIsEnforced() {
        var buffer = buffer().writeByteList(java.util.List.of((byte) 1, (byte) 2, (byte) 3));
        assertThrows(IllegalArgumentException.class, () -> buffer.readList(new ArrayList<>(), buffer::readByte, 2));
        var ok = buffer().writeByteList(java.util.List.of((byte) 1, (byte) 2));
        assertEquals(2, ok.readList(new ArrayList<>(), ok::readByte, 2).size());
    }

    @Test
    void elementWithoutProgressAborts() {
        var buffer = buffer().writeInt(3).writeInt(0).writeInt(0).writeInt(0);
        assertThrows(IllegalArgumentException.class, () -> buffer.readList(new ArrayList<>(), () -> "nothing read"));
        var map = buffer().writeInt(2).writeInt(0).writeInt(0);
        assertThrows(IllegalArgumentException.class, () -> map.readMap(new HashMap<>(), () -> "k", () -> "v"));
    }

    @Test
    void mapSizeAndMaxSizeAreChecked() {
        var huge = buffer().writeInt(1_000_000).writeInt(0);
        assertThrows(IllegalArgumentException.class, () -> huge.readMap(new HashMap<>(), huge::readString, huge::readString));
        var map = new HashMap<String, String>();
        map.put("a", "1");
        map.put("b", "2");
        var buffer = buffer().writeMap(map, CodecBuffer::writeString, CodecBuffer::writeString);
        assertThrows(IllegalArgumentException.class,
                () -> buffer.readMap(new HashMap<>(), buffer::readString, buffer::readString, 1));
    }

    @Test
    void enumOrdinalIsChecked() {
        assertEquals(Color.GREEN, buffer().writeEnum(Color.GREEN).readEnum(Color.class));
        assertThrows(IllegalArgumentException.class, () -> buffer().writeInt(2).readEnum(Color.class));
        assertThrows(IllegalArgumentException.class, () -> buffer().writeInt(-1).readEnum(Color.class));
    }

    @Test
    void jsonObjectLengthIsLimited() {
        var json = "{\"a\":\"" + "x".repeat(100) + "\"}";
        assertEquals(100, buffer().writeString(json).readJsonObject().get("a").getAsString().length());
        assertThrows(IllegalArgumentException.class, () -> buffer().writeString(json).readJsonObject(50));
        var tooLong = "{\"a\":\"" + "x".repeat(CodecBuffer.DEFAULT_MAX_JSON_LENGTH) + "\"}";
        assertThrows(IllegalArgumentException.class, () -> buffer().writeString(tooLong).readJsonObject());
    }

    @Test
    void errorsWhileConstructingAPacketCloseTheChannel() {
        for (var packetClass : java.util.List.<Class<? extends Packet>>of(ExplodingInitPacket.class,
                DeepRecursionPacket.class)) {
            var channel = channel(new PacketRegistry().register(packetClass));
            channel.attr(Netty5ChannelUtils.AUTHENTICATED).set(Boolean.TRUE);
            var error = assertThrows(DecoderException.class,
                    () -> channel.writeInbound(frame(packetClass.getName())), packetClass.getName());
            assertFalse(error instanceof PacketRejectedException, "a fatal error is not a mere rejection");
            assertFalse(channel.isOpen(), packetClass.getSimpleName() + " must close the channel");
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void safeLogCutsAndStripsControlCharacters() {
        assertEquals("a?b?c", SafeLog.clean("a" + (char) 13 + "b" + (char) 10 + "c"));
        assertEquals("x?y", SafeLog.clean("x" + (char) 0x2028 + "y"));
        var cleaned = SafeLog.clean("z".repeat(1000));
        assertEquals(SafeLog.MAX_LENGTH + 3, cleaned.length());
        assertEquals("null", SafeLog.clean(null));
        var channel = new EmbeddedChannel();
        var allowed = 0;
        for (int i = 0; i < 100; i++) {
            if (SafeLog.allow(channel)) {
                allowed++;
            }
        }
        assertEquals(SafeLog.LINES_PER_WINDOW, allowed);
        channel.finishAndReleaseAll();
    }

    private static EmbeddedChannel channel(PacketRegistry registry) {
        return new EmbeddedChannel(new Netty5ChannelInitializer(null, Netty5ChannelUtils.DEFAULT_MAX_FRAME_LENGTH,
                registry, false) {
            @Override
            public SimpleChannelInboundHandler<?> handler() {
                return new SimpleChannelInboundHandler<Packet>() {
                    @Override
                    protected void channelRead0(ChannelHandlerContext ctx, Packet msg) {
                    }
                };
            }
        });
    }

    private static io.netty.buffer.ByteBuf frame(String className) {
        var name = className.getBytes(StandardCharsets.UTF_8);
        var buffer = Unpooled.buffer();
        buffer.writeInt(Integer.BYTES + name.length + Integer.BYTES);
        buffer.writeInt(name.length);
        buffer.writeBytes(name);
        buffer.writeInt(0);
        return buffer;
    }
}

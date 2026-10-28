package de.dataflair.netty5.common.codec;

import de.dataflair.netty5.Netty5ClientChannel;
import de.dataflair.netty5.common.packet.Packet;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import java.nio.charset.StandardCharsets;

/**
 * Encodes a packet into {@code int nameLength, UTF-8 class name, int contentLength, content}.
 * <p>
 * The packet buffer is read without moving its reader index, so the same packet can be sent several times.
 * Exceptions are not swallowed: {@link MessageToByteEncoder} then releases the partially written buffer and
 * fails the write promise, so a partial frame never reaches the wire.
 */
public final class PacketEncoder extends MessageToByteEncoder<Packet> {
    private final Netty5ClientChannel.Identity identity;

    public PacketEncoder(Netty5ClientChannel.Identity identity) {
        super(Packet.class);
        this.identity = identity;
    }

    @Override
    protected ByteBuf allocateBuffer(ChannelHandlerContext ctx, Packet msg, boolean preferDirect) {
        var size = this.allocateBytes(msg);
        return preferDirect ? ctx.alloc().ioBuffer(size) : ctx.alloc().heapBuffer(size);
    }

    public int allocateBytes(Packet msg) {
        return Integer.BYTES +
                // class name
                msg.getClass().getName().getBytes(StandardCharsets.UTF_8).length +
                // amount of bytes in buffer
                Integer.BYTES +
                // buffer content
                msg.buffer().origin().readableBytes();
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, Packet msg, ByteBuf out) {
        try {
            var origin = msg.buffer().origin();
            var className = msg.getClass().getName().getBytes(StandardCharsets.UTF_8);
            var readableBytes = origin.readableBytes();

            out.writeInt(className.length);
            out.writeBytes(className);
            out.writeInt(readableBytes);
            out.writeBytes(origin, origin.readerIndex(), readableBytes);
        } catch (RuntimeException e) {
            System.err.println((identity != null ? "[identity: " + identity.name() + "] " : "")
                    + "Error while encoding packet " + msg.getClass().getName());
            throw e;
        }
    }
}

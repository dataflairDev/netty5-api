package de.dataflair.netty.common.codec;

import de.dataflair.netty.NettyChannelUtils;
import de.dataflair.netty.NettyClientChannel;
import de.dataflair.netty.common.packet.Packet;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import io.netty.handler.codec.TooLongFrameException;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;

/**
 * Encodes a packet into {@code int nameLength, UTF-8 class name, int contentLength, content}.
 * <p>
 * The packet buffer is read without moving its reader index, so the same packet can be sent several times.
 * Exceptions are not swallowed: {@link MessageToByteEncoder} then releases the partially written buffer and
 * fails the write promise, so a partial frame never reaches the wire. A packet whose frame (including the length
 * field) would exceed {@code maxFrameLength} fails the same way with a {@link TooLongFrameException}; the peer
 * would reject it anyway.
 */
@Slf4j
public final class PacketEncoder extends MessageToByteEncoder<Packet> {
    private final NettyClientChannel.Identity identity;
    private final int maxFrameLength;

    public PacketEncoder(NettyClientChannel.Identity identity) {
        this(identity, NettyChannelUtils.DEFAULT_MAX_FRAME_LENGTH);
    }

    /**
     * @param maxFrameLength maximum frame length in bytes including the length field
     */
    public PacketEncoder(NettyClientChannel.Identity identity, int maxFrameLength) {
        super(Packet.class);
        if (maxFrameLength <= 0) {
            throw new IllegalArgumentException("maxFrameLength must be positive: " + maxFrameLength);
        }
        this.identity = identity;
        this.maxFrameLength = maxFrameLength;
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
            var frameLength = (long) Integer.BYTES + Integer.BYTES + className.length + Integer.BYTES + readableBytes;
            if (frameLength > maxFrameLength) {
                throw new TooLongFrameException("Packet " + msg.getClass().getName() + " needs a frame of "
                        + frameLength + " bytes, maximum is " + maxFrameLength);
            }

            out.writeInt(className.length);
            out.writeBytes(className);
            out.writeInt(readableBytes);
            out.writeBytes(origin, origin.readerIndex(), readableBytes);
        } catch (RuntimeException e) {
            log.warn((identity != null ? "[identity: " + identity.name() + "] " : "")
                    + "Error while encoding packet " + msg.getClass().getName());
            throw e;
        }
    }
}

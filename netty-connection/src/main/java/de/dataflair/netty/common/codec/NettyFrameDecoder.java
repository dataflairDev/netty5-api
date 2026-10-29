package de.dataflair.netty.common.codec;

import de.dataflair.netty.NettyChannelUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.TooLongFrameException;

import java.nio.ByteOrder;

/**
 * Splits the stream into frames ({@code int length} followed by {@code length} bytes).
 * <p>
 * While a channel that requires authentication is not authenticated, the announced length is checked against
 * {@code preAuthMaxFrameLength} as soon as the length field has arrived, before any content is buffered. A
 * longer frame closes the channel with a {@link TooLongFrameException}. After authentication the limit is
 * {@code maxFrameLength}. While an asynchronous connection filter is running no frame is cut;
 * {@link #resume(ChannelHandlerContext)} continues with the buffered bytes.
 */
public final class NettyFrameDecoder extends LengthFieldBasedFrameDecoder {
    private final boolean requireAuthentication;
    private final int preAuthMaxFrameLength;
    private ChannelHandlerContext context;

    public NettyFrameDecoder(int maxFrameLength, int preAuthMaxFrameLength, boolean requireAuthentication) {
        super(maxFrameLength, 0, Integer.BYTES, 0, Integer.BYTES);
        if (preAuthMaxFrameLength <= 0) {
            throw new IllegalArgumentException("preAuthMaxFrameLength must be positive: " + preAuthMaxFrameLength);
        }
        this.requireAuthentication = requireAuthentication;
        this.preAuthMaxFrameLength = preAuthMaxFrameLength;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        this.context = ctx;
        super.handlerAdded(ctx);
    }

    @Override
    protected Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
        var channel = ctx.channel();
        if (!channel.isActive()) {
            in.skipBytes(in.readableBytes());
            return null;
        }
        if (NettyChannelUtils.isAuthPending(channel)) {
            // keep the bytes; reading is suspended until the filter has decided
            return null;
        }
        return super.decode(ctx, in);
    }

    @Override
    protected long getUnadjustedFrameLength(ByteBuf buf, int offset, int length, ByteOrder order) {
        var frameLength = super.getUnadjustedFrameLength(buf, offset, length, order);
        var context = this.context;
        if (requireAuthentication && context != null && !NettyChannelUtils.isAuthenticated(context.channel())
                && frameLength + Integer.BYTES > preAuthMaxFrameLength) {
            context.close();
            throw new TooLongFrameException("Frame of " + (frameLength + Integer.BYTES)
                    + " bytes before authentication exceeds " + preAuthMaxFrameLength + " bytes");
        }
        return frameLength;
    }

    /**
     * Cuts the frames that arrived while the authentication was pending. Must be called on the event loop.
     */
    public void resume(ChannelHandlerContext ctx) throws Exception {
        channelRead(ctx, Unpooled.EMPTY_BUFFER);
        ctx.fireChannelReadComplete();
    }
}

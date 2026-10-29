package de.dataflair.netty;

import de.dataflair.netty.common.log.SafeLog;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import lombok.extern.slf4j.Slf4j;

/**
 * Bounds the outbound buffer of a channel.
 * <ul>
 *     <li>When the channel becomes unwritable (pending bytes above the high water mark, see
 *     {@link io.netty.channel.ChannelOption#WRITE_BUFFER_WATER_MARK}) reading is suspended
 *     ({@code autoRead=false}), so a peer that does not read cannot make us produce more responses. Reading
 *     resumes when the channel is writable again. Only on the server ({@code suspendReadWhenUnwritable}): if both
 *     peers stopped reading while unwritable, neither would ever drain the other's buffer, so the client keeps
 *     reading and relies on the hard limit below.</li>
 *     <li>If the pending bytes exceed {@code maxOutboundBufferBytes} anyway (for example because the application
 *     keeps writing on its own), the channel is closed.</li>
 * </ul>
 */
@Slf4j
public final class BackpressureHandler extends ChannelDuplexHandler {

    private final long maxOutboundBufferBytes;
    private final boolean suspendReadWhenUnwritable;

    public BackpressureHandler(long maxOutboundBufferBytes) {
        this(maxOutboundBufferBytes, true);
    }

    /**
     * @param maxOutboundBufferBytes    pending bytes above which the channel is closed
     * @param suspendReadWhenUnwritable if true reading is suspended while the channel is unwritable (server side)
     */
    public BackpressureHandler(long maxOutboundBufferBytes, boolean suspendReadWhenUnwritable) {
        if (maxOutboundBufferBytes <= 0) {
            throw new IllegalArgumentException("maxOutboundBufferBytes must be positive: " + maxOutboundBufferBytes);
        }
        this.maxOutboundBufferBytes = maxOutboundBufferBytes;
        this.suspendReadWhenUnwritable = suspendReadWhenUnwritable;
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
        var channel = ctx.channel();
        if (!suspendReadWhenUnwritable) {
            super.channelWritabilityChanged(ctx);
            return;
        }
        if (channel.isWritable()) {
            NettyChannelUtils.resumeRead(channel, NettyChannelUtils.READ_SUSPENDED_BACKPRESSURE);
        } else {
            NettyChannelUtils.suspendRead(channel, NettyChannelUtils.READ_SUSPENDED_BACKPRESSURE);
        }
        super.channelWritabilityChanged(ctx);
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        super.write(ctx, msg, promise);
        var channel = ctx.channel();
        var outbound = channel.unsafe().outboundBuffer();
        if (outbound != null && channel.isActive() && outbound.totalPendingWriteBytes() > maxOutboundBufferBytes) {
            if (SafeLog.allow(channel)) {
                log.warn("Outbound buffer of {} exceeds {} bytes ({} pending), closing", channel.remoteAddress(),
                        maxOutboundBufferBytes, outbound.totalPendingWriteBytes());
            }
            ctx.close();
        }
    }
}

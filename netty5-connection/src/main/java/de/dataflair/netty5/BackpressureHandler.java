package de.dataflair.netty5;

import de.dataflair.netty5.common.log.SafeLog;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bounds the outbound buffer of a channel.
 * <ul>
 *     <li>When the channel becomes unwritable (pending bytes above the high water mark, see
 *     {@link io.netty.channel.ChannelOption#WRITE_BUFFER_WATER_MARK}) reading is suspended
 *     ({@code autoRead=false}), so a peer that does not read cannot make us produce more responses. Reading
 *     resumes when the channel is writable again.</li>
 *     <li>If the pending bytes exceed {@code maxOutboundBufferBytes} anyway (for example because the application
 *     keeps writing on its own), the channel is closed.</li>
 * </ul>
 */
public final class BackpressureHandler extends ChannelDuplexHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(BackpressureHandler.class);

    private final long maxOutboundBufferBytes;

    public BackpressureHandler(long maxOutboundBufferBytes) {
        if (maxOutboundBufferBytes <= 0) {
            throw new IllegalArgumentException("maxOutboundBufferBytes must be positive: " + maxOutboundBufferBytes);
        }
        this.maxOutboundBufferBytes = maxOutboundBufferBytes;
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
        var channel = ctx.channel();
        if (channel.isWritable()) {
            Netty5ChannelUtils.resumeRead(channel, Netty5ChannelUtils.READ_SUSPENDED_BACKPRESSURE);
        } else {
            Netty5ChannelUtils.suspendRead(channel, Netty5ChannelUtils.READ_SUSPENDED_BACKPRESSURE);
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
                LOGGER.warn("Outbound buffer of {} exceeds {} bytes ({} pending), closing", channel.remoteAddress(),
                        maxOutboundBufferBytes, outbound.totalPendingWriteBytes());
            }
            ctx.close();
        }
    }
}

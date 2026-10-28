package de.dataflair.netty5.server;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * First handler of every server side connection. Counts open connections globally and per remote address and
 * closes a new connection right away if a limit is exceeded or its address is blocked by the
 * {@link AuthFailureLimiter}.
 */
@ChannelHandler.Sharable
public final class ConnectionLimiter extends ChannelInboundHandlerAdapter {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionLimiter.class);
    private static final long LOG_INTERVAL_MILLIS = 1_000;

    private final int maxConnections;
    private final int maxConnectionsPerAddress;
    private final AuthFailureLimiter authFailureLimiter;
    private final AtomicInteger total = new AtomicInteger();
    private final Map<InetAddress, Integer> perAddress = new ConcurrentHashMap<>();
    private final AtomicLong lastLog = new AtomicLong();

    public ConnectionLimiter(int maxConnections, int maxConnectionsPerAddress,
                             @NotNull AuthFailureLimiter authFailureLimiter) {
        if (maxConnections <= 0 || maxConnectionsPerAddress <= 0) {
            throw new IllegalArgumentException("connection limits must be positive: " + maxConnections + ", "
                    + maxConnectionsPerAddress);
        }
        this.maxConnections = maxConnections;
        this.maxConnectionsPerAddress = maxConnectionsPerAddress;
        this.authFailureLimiter = authFailureLimiter;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        var channel = ctx.channel();
        var address = address(channel);
        if (address != null && authFailureLimiter.isBlocked(address)) {
            log("Address {} is blocked after failed authentications, closing", address);
            ctx.close();
            return;
        }
        if (total.incrementAndGet() > maxConnections) {
            total.decrementAndGet();
            log("Connection limit of {} reached, closing connection from {}", maxConnections, address);
            ctx.close();
            return;
        }
        if (address != null) {
            var count = perAddress.merge(address, 1, Integer::sum);
            if (count > maxConnectionsPerAddress) {
                release(address);
                log("Connection limit of {} per address reached for {}, closing", maxConnectionsPerAddress, address);
                ctx.close();
                return;
            }
        }
        channel.closeFuture().addListener(_ -> release(address));
        super.channelActive(ctx);
    }

    /**
     * @return the number of counted open connections
     */
    public int connections() {
        return total.get();
    }

    private void release(@Nullable InetAddress address) {
        total.decrementAndGet();
        if (address != null) {
            perAddress.computeIfPresent(address, (_, count) -> count <= 1 ? null : count - 1);
        }
    }

    private static @Nullable InetAddress address(Channel channel) {
        return channel.remoteAddress() instanceof InetSocketAddress socketAddress ? socketAddress.getAddress() : null;
    }

    // at most one line per second, a connection flood must not flood the log
    private void log(String message, Object... arguments) {
        var now = System.currentTimeMillis();
        var last = lastLog.get();
        if (now - last >= LOG_INTERVAL_MILLIS && lastLog.compareAndSet(last, now)) {
            LOGGER.warn(message, arguments);
        }
    }
}

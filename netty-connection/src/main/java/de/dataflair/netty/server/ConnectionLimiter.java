package de.dataflair.netty5.server;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.AttributeKey;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * First handler of every server side connection. A new connection is closed right away if
 * <ul>
 *     <li>its prefix is blocked by the {@link AuthFailureLimiter},</li>
 *     <li>its prefix opened more than its budget of new connections ({@code connectionRateLimiter}),</li>
 *     <li>{@code maxConnections} connections are open,</li>
 *     <li>{@code maxUnauthenticatedConnections} connections have not authenticated yet, or</li>
 *     <li>{@code maxConnectionsPerAddress} connections of its prefix are open.</li>
 * </ul>
 * Prefixes are IPv4 /32 and IPv6 {@code ipv6PrefixLength} (default /64).
 */
@ChannelHandler.Sharable
public final class ConnectionLimiter extends ChannelInboundHandlerAdapter {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionLimiter.class);
    private static final long LOG_INTERVAL_MILLIS = 1_000;
    private static final AttributeKey<AtomicBoolean> PRE_AUTH_SLOT =
            AttributeKey.valueOf("netty5-api.pre-auth-slot");
    private static final AttributeKey<Boolean> ADMITTED = AttributeKey.valueOf("netty5-api.admitted");

    private final int maxConnections;
    private final int maxConnectionsPerAddress;
    private final int maxUnauthenticatedConnections;
    private final int ipv6PrefixLength;
    private final AuthFailureLimiter authFailureLimiter;
    private final PrefixRateLimiter connectionRateLimiter;
    private final AtomicInteger total = new AtomicInteger();
    private final AtomicInteger unauthenticated = new AtomicInteger();
    private final Map<InetAddress, Integer> perAddress = new ConcurrentHashMap<>();
    private final AtomicLong lastLog = new AtomicLong();

    /**
     * Without pre-auth slot limit and connection rate limit.
     */
    public ConnectionLimiter(int maxConnections, int maxConnectionsPerAddress,
                             @NotNull AuthFailureLimiter authFailureLimiter) {
        this(maxConnections, maxConnectionsPerAddress, maxConnections, authFailureLimiter,
                new PrefixRateLimiter(0, 1, PrefixRateLimiter.DEFAULT_IPV6_PREFIX_LENGTH, 1),
                PrefixRateLimiter.DEFAULT_IPV6_PREFIX_LENGTH);
    }

    public ConnectionLimiter(int maxConnections, int maxConnectionsPerAddress, int maxUnauthenticatedConnections,
                             @NotNull AuthFailureLimiter authFailureLimiter,
                             @NotNull PrefixRateLimiter connectionRateLimiter, int ipv6PrefixLength) {
        if (maxConnections <= 0 || maxConnectionsPerAddress <= 0 || maxUnauthenticatedConnections <= 0) {
            throw new IllegalArgumentException("connection limits must be positive: " + maxConnections + ", "
                    + maxConnectionsPerAddress + ", " + maxUnauthenticatedConnections);
        }
        this.maxConnections = maxConnections;
        this.maxConnectionsPerAddress = maxConnectionsPerAddress;
        this.maxUnauthenticatedConnections = maxUnauthenticatedConnections;
        this.ipv6PrefixLength = ipv6PrefixLength;
        this.authFailureLimiter = authFailureLimiter;
        this.connectionRateLimiter = connectionRateLimiter;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        var channel = ctx.channel();
        var address = address(channel);
        if (address != null && authFailureLimiter.isBlocked(address)) {
            log("Prefix of {} is blocked after failed authentications, closing", address);
            ctx.close();
            return;
        }
        if (address != null && !connectionRateLimiter.tryAcquire(address)) {
            log("Prefix of {} opens connections too fast, closing", address);
            ctx.close();
            return;
        }
        if (total.incrementAndGet() > maxConnections) {
            total.decrementAndGet();
            log("Connection limit of {} reached, closing connection from {}", maxConnections, address);
            ctx.close();
            return;
        }
        if (unauthenticated.incrementAndGet() > maxUnauthenticatedConnections) {
            unauthenticated.decrementAndGet();
            total.decrementAndGet();
            log("Limit of {} unauthenticated connections reached, closing connection from {}",
                    maxUnauthenticatedConnections, address);
            ctx.close();
            return;
        }
        var slot = new AtomicBoolean(true);
        var prefix = address == null ? null : PrefixRateLimiter.prefix(address, ipv6PrefixLength);
        if (prefix != null) {
            var count = perAddress.merge(prefix, 1, Integer::sum);
            if (count > maxConnectionsPerAddress) {
                release(prefix, slot);
                log("Connection limit of {} per prefix reached for {}, closing", maxConnectionsPerAddress, address);
                ctx.close();
                return;
            }
        }
        channel.attr(PRE_AUTH_SLOT).set(slot);
        channel.attr(ADMITTED).set(Boolean.TRUE);
        channel.closeFuture().addListener(_ -> release(prefix, slot));
        super.channelActive(ctx);
    }

    /**
     * Frees the pre-auth slot of an authenticated channel. Safe to call more than once.
     */
    public void authenticated(@NotNull Channel channel) {
        var slot = channel.attr(PRE_AUTH_SLOT).get();
        if (slot != null && slot.compareAndSet(true, false)) {
            unauthenticated.decrementAndGet();
        }
    }

    /**
     * @return true if this limiter let the channel through
     */
    public static boolean isAdmitted(@NotNull Channel channel) {
        return Boolean.TRUE.equals(channel.attr(ADMITTED).get());
    }

    /**
     * @return the number of counted open connections
     */
    public int connections() {
        return total.get();
    }

    /**
     * @return the number of counted connections that have not authenticated yet
     */
    public int unauthenticatedConnections() {
        return unauthenticated.get();
    }

    private void release(@Nullable InetAddress prefix, AtomicBoolean slot) {
        total.decrementAndGet();
        if (slot.compareAndSet(true, false)) {
            unauthenticated.decrementAndGet();
        }
        if (prefix != null) {
            perAddress.computeIfPresent(prefix, (_, count) -> count <= 1 ? null : count - 1);
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

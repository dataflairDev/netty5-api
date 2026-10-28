package de.dataflair.netty5.server;

import org.jetbrains.annotations.NotNull;

import java.net.InetAddress;

/**
 * Throttles authentication attempts per network prefix (IPv4 /32, IPv6 /64 by default, see
 * {@link PrefixRateLimiter}) with a token bucket of {@code maxFailures} tokens per minute.
 * <p>
 * Every attempt takes a token atomically when it starts ({@link #tryAcquire}); a successful authentication gives
 * it back ({@link #release}). Parallel attempts therefore cannot all pass a check before the first failure is
 * counted: per prefix at most {@code maxFailures} attempts are evaluated at the same time or fail per minute. A
 * prefix without tokens is blocked, new connections from it are closed right away. Failed TLS handshakes are
 * counted with {@link #recordFailure}.
 */
public final class AuthFailureLimiter {
    static final long WINDOW_MILLIS = 60_000;

    private final PrefixRateLimiter limiter;

    /**
     * @param maxFailures failed authentications per minute and prefix; 0 or less disables the limiter
     */
    public AuthFailureLimiter(int maxFailures) {
        this(maxFailures, PrefixRateLimiter.DEFAULT_IPV6_PREFIX_LENGTH, PrefixRateLimiter.DEFAULT_MAX_TRACKED_PREFIXES);
    }

    /**
     * @param maxFailures        failed authentications per minute and prefix; 0 or less disables the limiter
     * @param ipv6PrefixLength   bits of an IPv6 address that form the key
     * @param maxTrackedPrefixes maximum number of tracked prefixes
     */
    public AuthFailureLimiter(int maxFailures, int ipv6PrefixLength, int maxTrackedPrefixes) {
        this(new PrefixRateLimiter(maxFailures, WINDOW_MILLIS, ipv6PrefixLength, maxTrackedPrefixes));
    }

    AuthFailureLimiter(@NotNull PrefixRateLimiter limiter) {
        this.limiter = limiter;
    }

    /**
     * Starts an authentication attempt.
     *
     * @return false if the prefix has used up its attempts; the attempt must then be rejected
     */
    public boolean tryAcquire(@NotNull InetAddress address) {
        return limiter.tryAcquire(address);
    }

    /**
     * Gives the token of an attempt back that did not fail.
     */
    public void release(@NotNull InetAddress address) {
        limiter.release(address);
    }

    /**
     * Counts a failure that did not start with {@link #tryAcquire}, for example a failed TLS handshake.
     */
    public void recordFailure(@NotNull InetAddress address) {
        limiter.consume(address);
    }

    /**
     * @return true if the prefix of the address has used up its attempts
     */
    public boolean isBlocked(@NotNull InetAddress address) {
        return limiter.isBlocked(address);
    }

    /**
     * Removes the prefixes whose budget has refilled completely.
     *
     * @return the number of removed prefixes
     */
    public int cleanup() {
        return limiter.cleanup();
    }

    public int trackedPrefixes() {
        return limiter.trackedPrefixes();
    }
}

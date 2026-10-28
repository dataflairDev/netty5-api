package de.dataflair.netty5.server;

import org.jetbrains.annotations.NotNull;

import java.net.InetAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Throttles failed authentications per remote address with a token bucket: {@code maxFailures} failures per
 * minute are allowed, then the address is rejected (new connections are closed right away, auth packets are
 * not passed to the filters) until the bucket has refilled enough for the next attempt.
 */
public final class AuthFailureLimiter {
    private static final long WINDOW_MILLIS = 60_000;
    private static final int MAX_TRACKED_ADDRESSES = 100_000;

    private final int maxFailures;
    private final Map<InetAddress, Bucket> buckets = new ConcurrentHashMap<>();

    /**
     * @param maxFailures failed authentications per minute and address; 0 or less disables the limiter
     */
    public AuthFailureLimiter(int maxFailures) {
        this.maxFailures = maxFailures;
    }

    /**
     * @return true if the address used up its failed authentications
     */
    public boolean isBlocked(@NotNull InetAddress address) {
        if (maxFailures <= 0) {
            return false;
        }
        var bucket = buckets.get(address);
        return bucket != null && bucket.blocked(System.currentTimeMillis());
    }

    public void recordFailure(@NotNull InetAddress address) {
        if (maxFailures <= 0) {
            return;
        }
        var now = System.currentTimeMillis();
        if (buckets.size() >= MAX_TRACKED_ADDRESSES) {
            buckets.values().removeIf(bucket -> bucket.full(now));
        }
        buckets.computeIfAbsent(address, _ -> new Bucket(now)).consume(now);
    }

    private final class Bucket {
        private double tokens = maxFailures;
        private long last;

        Bucket(long now) {
            this.last = now;
        }

        private void refill(long now) {
            tokens = Math.min(maxFailures, tokens + (now - last) * (double) maxFailures / WINDOW_MILLIS);
            last = now;
        }

        synchronized boolean blocked(long now) {
            refill(now);
            return tokens < 1;
        }

        synchronized boolean full(long now) {
            refill(now);
            return tokens >= maxFailures;
        }

        synchronized void consume(long now) {
            refill(now);
            tokens = Math.max(0, tokens - 1);
        }
    }
}

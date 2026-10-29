package de.dataflair.netty.server;

import org.jetbrains.annotations.NotNull;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.LongSupplier;

/**
 * Token bucket per network prefix: every prefix may spend {@code capacity} tokens per {@code windowMillis}, the
 * bucket refills continuously. IPv4 addresses are tracked per address (/32), IPv6 addresses per
 * {@code ipv6PrefixLength} bits (default /64), so rotating through the addresses of one IPv6 subnet does not give
 * a peer a fresh budget.
 * <p>
 * At most {@code maxTrackedPrefixes} buckets are kept; when a new prefix would exceed that, the least recently
 * used bucket is dropped. {@link #cleanup()} removes buckets that have refilled completely and is meant to be
 * called periodically (the server schedules it on its event loop), so a failure does not cost a scan over all
 * entries. All methods are thread-safe.
 */
public final class PrefixRateLimiter {
    public static final int DEFAULT_IPV6_PREFIX_LENGTH = 64;
    public static final int DEFAULT_MAX_TRACKED_PREFIXES = 100_000;

    private final int capacity;
    private final long windowMillis;
    private final int ipv6PrefixLength;
    private final int maxTrackedPrefixes;
    private final LongSupplier clock;
    private final LinkedHashMap<InetAddress, Bucket> buckets;

    /**
     * @param capacity           tokens per window and prefix; 0 or less disables the limiter
     * @param windowMillis       time in which an empty bucket refills completely
     * @param ipv6PrefixLength   bits of an IPv6 address that form the key (1 to 128)
     * @param maxTrackedPrefixes maximum number of buckets kept at the same time
     */
    public PrefixRateLimiter(int capacity, long windowMillis, int ipv6PrefixLength, int maxTrackedPrefixes) {
        this(capacity, windowMillis, ipv6PrefixLength, maxTrackedPrefixes, System::currentTimeMillis);
    }

    PrefixRateLimiter(int capacity, long windowMillis, int ipv6PrefixLength, int maxTrackedPrefixes,
                      @NotNull LongSupplier clock) {
        if (windowMillis <= 0) {
            throw new IllegalArgumentException("windowMillis must be positive: " + windowMillis);
        }
        if (ipv6PrefixLength < 1 || ipv6PrefixLength > 128) {
            throw new IllegalArgumentException("ipv6PrefixLength must be between 1 and 128: " + ipv6PrefixLength);
        }
        if (maxTrackedPrefixes <= 0) {
            throw new IllegalArgumentException("maxTrackedPrefixes must be positive: " + maxTrackedPrefixes);
        }
        this.capacity = capacity;
        this.windowMillis = windowMillis;
        this.ipv6PrefixLength = ipv6PrefixLength;
        this.maxTrackedPrefixes = maxTrackedPrefixes;
        this.clock = clock;
        this.buckets = new LinkedHashMap<>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<InetAddress, Bucket> eldest) {
                return size() > PrefixRateLimiter.this.maxTrackedPrefixes;
            }
        };
    }

    public boolean enabled() {
        return capacity > 0;
    }

    /**
     * Takes one token of the prefix of {@code address}.
     *
     * @return false (and nothing is taken) if the prefix has no token left
     */
    public boolean tryAcquire(@NotNull InetAddress address) {
        if (!enabled()) {
            return true;
        }
        var key = key(address);
        var now = clock.getAsLong();
        synchronized (buckets) {
            var bucket = buckets.get(key);
            if (bucket == null) {
                bucket = new Bucket(now);
                buckets.put(key, bucket);
            }
            bucket.refill(now);
            if (bucket.tokens < 1) {
                return false;
            }
            bucket.tokens -= 1;
            return true;
        }
    }

    /**
     * Takes one token of the prefix of {@code address} even if that leaves the bucket empty.
     */
    public void consume(@NotNull InetAddress address) {
        if (!enabled()) {
            return;
        }
        var key = key(address);
        var now = clock.getAsLong();
        synchronized (buckets) {
            var bucket = buckets.get(key);
            if (bucket == null) {
                bucket = new Bucket(now);
                buckets.put(key, bucket);
            }
            bucket.refill(now);
            bucket.tokens = Math.max(0, bucket.tokens - 1);
        }
    }

    /**
     * Gives one token back, for example after an attempt turned out to be legitimate.
     */
    public void release(@NotNull InetAddress address) {
        if (!enabled()) {
            return;
        }
        var key = key(address);
        var now = clock.getAsLong();
        synchronized (buckets) {
            var bucket = buckets.get(key);
            if (bucket != null) {
                bucket.refill(now);
                bucket.tokens = Math.min(capacity, bucket.tokens + 1);
            }
        }
    }

    /**
     * @return true if the prefix of {@code address} has no token left
     */
    public boolean isBlocked(@NotNull InetAddress address) {
        if (!enabled()) {
            return false;
        }
        var key = key(address);
        var now = clock.getAsLong();
        synchronized (buckets) {
            var bucket = buckets.get(key);
            if (bucket == null) {
                return false;
            }
            bucket.refill(now);
            return bucket.tokens < 1;
        }
    }

    /**
     * Removes the buckets that have refilled completely.
     *
     * @return the number of removed buckets
     */
    public int cleanup() {
        var now = clock.getAsLong();
        synchronized (buckets) {
            var before = buckets.size();
            buckets.values().removeIf(bucket -> {
                bucket.refill(now);
                return bucket.tokens >= capacity;
            });
            return before - buckets.size();
        }
    }

    /**
     * @return the number of buckets currently kept
     */
    public int trackedPrefixes() {
        synchronized (buckets) {
            return buckets.size();
        }
    }

    /**
     * @return the key {@code address} is tracked under: the address itself for IPv4, the first
     * {@code ipv6PrefixLength} bits for IPv6
     */
    public @NotNull InetAddress key(@NotNull InetAddress address) {
        return prefix(address, ipv6PrefixLength);
    }

    /**
     * @return {@code address} itself for IPv4, its first {@code ipv6PrefixLength} bits (the rest zeroed) for IPv6
     */
    public static @NotNull InetAddress prefix(@NotNull InetAddress address, int ipv6PrefixLength) {
        if (!(address instanceof Inet6Address)) {
            return address;
        }
        var bytes = address.getAddress();
        for (var bit = Math.max(0, ipv6PrefixLength); bit < 128; bit++) {
            bytes[bit / 8] &= (byte) ~(0x80 >>> (bit % 8));
        }
        try {
            // without scope id: link-local peers on different interfaces share a prefix as well
            return InetAddress.getByAddress(bytes);
        } catch (UnknownHostException e) {
            throw new IllegalStateException(e);
        }
    }

    private final class Bucket {
        private double tokens = capacity;
        private long last;

        Bucket(long now) {
            this.last = now;
        }

        void refill(long now) {
            if (now > last) {
                tokens = Math.min(capacity, tokens + (now - last) * (double) capacity / windowMillis);
                last = now;
            }
        }
    }
}

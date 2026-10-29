package de.dataflair.netty5.server;

import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PrefixRateLimiterTest {

    @Test
    void ipv6AddressesOfOneSlash64ShareABudget() throws Exception {
        var limiter = new PrefixRateLimiter(2, 60_000, 64, 1_000);
        assertTrue(limiter.tryAcquire(InetAddress.getByName("2001:db8:1:2::1")));
        assertTrue(limiter.tryAcquire(InetAddress.getByName("2001:db8:1:2:ffff:ffff:ffff:fffe")));
        // rotating the interface identifier does not help
        assertFalse(limiter.tryAcquire(InetAddress.getByName("2001:db8:1:2:abcd::99")));
        assertTrue(limiter.isBlocked(InetAddress.getByName("2001:db8:1:2::7")));
        // the next /64 has its own budget
        assertTrue(limiter.tryAcquire(InetAddress.getByName("2001:db8:1:3::1")));
        assertEquals(2, limiter.trackedPrefixes());
    }

    @Test
    void ipv6PrefixLengthIsConfigurable() throws Exception {
        var limiter = new PrefixRateLimiter(1, 60_000, 48, 1_000);
        assertTrue(limiter.tryAcquire(InetAddress.getByName("2001:db8:1:2::1")));
        assertFalse(limiter.tryAcquire(InetAddress.getByName("2001:db8:1:ff00::1")), "same /48");
        assertEquals(InetAddress.getByName("2001:db8:1::"),
                PrefixRateLimiter.prefix(InetAddress.getByName("2001:db8:1:2:3:4:5:6"), 48));
        assertThrows(IllegalArgumentException.class, () -> new PrefixRateLimiter(1, 1, 0, 1));
        assertThrows(IllegalArgumentException.class, () -> new PrefixRateLimiter(1, 1, 129, 1));
    }

    @Test
    void ipv4IsTrackedPerAddress() throws Exception {
        var limiter = new PrefixRateLimiter(1, 60_000, 64, 1_000);
        assertTrue(limiter.tryAcquire(InetAddress.getByName("192.0.2.1")));
        assertFalse(limiter.tryAcquire(InetAddress.getByName("192.0.2.1")));
        assertTrue(limiter.tryAcquire(InetAddress.getByName("192.0.2.2")));
    }

    @Test
    void trackedPrefixesAreBoundedAndTheLeastRecentlyUsedIsDropped() throws Exception {
        var limiter = new PrefixRateLimiter(1, 60_000, 64, 3);
        var a = InetAddress.getByName("198.51.100.1");
        assertTrue(limiter.tryAcquire(a));
        for (int i = 2; i <= 50; i++) {
            limiter.tryAcquire(InetAddress.getByName("198.51.100." + i));
            limiter.isBlocked(a); // keeps a recently used
            assertTrue(limiter.trackedPrefixes() <= 3);
        }
        assertTrue(limiter.isBlocked(a), "the recently used prefix survives");
        assertFalse(limiter.isBlocked(InetAddress.getByName("198.51.100.2")), "an old prefix was dropped");
    }

    @Test
    void cleanupRemovesRefilledPrefixes() throws Exception {
        var now = new AtomicLong(1_000);
        var limiter = new PrefixRateLimiter(2, 10_000, 64, 1_000, now::get);
        limiter.tryAcquire(InetAddress.getByName("203.0.113.1"));
        limiter.tryAcquire(InetAddress.getByName("203.0.113.2"));
        limiter.tryAcquire(InetAddress.getByName("203.0.113.2"));
        assertEquals(0, limiter.cleanup(), "nothing refilled yet");
        now.addAndGet(5_000);
        assertEquals(1, limiter.cleanup(), "one token per 5 s: only .1 is full again");
        assertEquals(1, limiter.trackedPrefixes());
        now.addAndGet(5_000);
        assertEquals(1, limiter.cleanup());
        assertEquals(0, limiter.trackedPrefixes());
    }

    @Test
    void releaseGivesATokenBack() throws Exception {
        var limiter = new PrefixRateLimiter(1, 60_000, 64, 10);
        var address = InetAddress.getByName("192.0.2.9");
        assertTrue(limiter.tryAcquire(address));
        assertTrue(limiter.isBlocked(address));
        limiter.release(address);
        assertFalse(limiter.isBlocked(address));
        assertTrue(limiter.tryAcquire(address));
    }

    @Test
    void parallelAcquiresNeverExceedTheCapacity() throws Exception {
        var limiter = new AuthFailureLimiter(5);
        var address = InetAddress.getByName("192.0.2.77");
        var granted = new AtomicInteger();
        var start = new CountDownLatch(1);
        var threads = new ArrayList<Thread>();
        for (int i = 0; i < 64; i++) {
            threads.add(Thread.ofPlatform().start(() -> {
                try {
                    start.await();
                } catch (InterruptedException e) {
                    return;
                }
                if (limiter.tryAcquire(address)) {
                    granted.incrementAndGet();
                }
            }));
        }
        start.countDown();
        for (var thread : threads) {
            thread.join();
        }
        assertEquals(5, granted.get());
        assertTrue(limiter.isBlocked(address));
    }

    @Test
    void disabledLimiterAllowsEverything() throws Exception {
        var limiter = new PrefixRateLimiter(0, 1_000, 64, 1);
        var address = InetAddress.getByName("192.0.2.1");
        for (int i = 0; i < 100; i++) {
            assertTrue(limiter.tryAcquire(address));
        }
        assertFalse(limiter.isBlocked(address));
        assertEquals(0, limiter.trackedPrefixes());
    }
}

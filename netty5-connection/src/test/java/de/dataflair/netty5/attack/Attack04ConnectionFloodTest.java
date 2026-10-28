package de.dataflair.netty5.attack;

import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.net.Socket;
import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Attack 4 - 2000 connections that never send a byte. They must be closed by the auth timeout, without leaking
 * threads (the event loop group is fixed-size) and without leaving authenticated connections behind.
 */
class Attack04ConnectionFloodTest {

    private static final int TARGET = 2000;

    @Test
    void idleConnectionsAreClosedByAuthTimeoutWithoutThreadLeak() throws Exception {
        // 2 s auth timeout; per-address limit raised so this test measures the auth timeout, not the limiter
        var harness = AttackHarness.start(false, 2_000, server -> server.maxConnectionsPerAddress(5_000));
        var sockets = new ArrayList<Socket>();
        try {
            int threadsBefore = ManagementFactory.getThreadMXBean().getThreadCount();

            int opened = 0;
            for (int i = 0; i < TARGET; i++) {
                try {
                    sockets.add(harness.connect());
                    opened++;
                } catch (Exception e) {
                    break; // OS/backlog limit reached; record how far we got
                }
            }
            int threadsAfterOpen = ManagementFactory.getThreadMXBean().getThreadCount();

            // wait past the auth timeout, then count how many the server closed
            Thread.sleep(4_000);
            int closed = 0;
            for (var s : sockets) {
                try {
                    s.setSoTimeout(1_000);
                    if (s.getInputStream().read() == -1) {
                        closed++;
                    }
                } catch (Exception e) {
                    // read timeout or reset: not observed as cleanly closed
                }
            }
            int threadsAfterClose = ManagementFactory.getThreadMXBean().getThreadCount();

            System.out.println("[attack4] opened=" + opened + " closed-by-timeout=" + closed
                    + " threads before=" + threadsBefore + " afterOpen=" + threadsAfterOpen
                    + " afterClose=" + threadsAfterClose);

            assertEquals(0, harness.server().connections().size(), "no idle connection may become authenticated");
            assertTrue(opened >= 500, "expected to open a substantial number of connections, got " + opened);
            assertTrue(closed >= opened * 9 / 10,
                    "auth timeout should close almost all idle connections (" + closed + " of " + opened + ")");
            // the event loop group is fixed-size (~2x CPU cores); threads must be bounded by it, not by the
            // connection count, and must not keep growing after the connections close
            int poolBound = Runtime.getRuntime().availableProcessors() * 2 + 32;
            assertTrue(threadsAfterOpen - threadsBefore <= poolBound,
                    "thread count must be bounded by the fixed event loop pool, not the connection count (delta "
                            + (threadsAfterOpen - threadsBefore) + ", bound " + poolBound + ")");
            assertTrue(threadsAfterClose <= threadsAfterOpen,
                    "thread count must not keep growing (afterOpen=" + threadsAfterOpen
                            + ", afterClose=" + threadsAfterClose + ")");
        } finally {
            for (var s : sockets) {
                try {
                    s.close();
                } catch (Exception ignored) {
                }
            }
            harness.stop();
        }
    }
}

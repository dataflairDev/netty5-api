package de.dataflair.netty5.attack;

import de.dataflair.netty5.testpackets.TestPacket;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufAllocatorMetricProvider;
import org.junit.jupiter.api.Test;

import java.net.Socket;
import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Attack 3 - Pre-auth memory. 50 unauthenticated connections each announce a ~15 MiB frame (just under the
 * 16 MiB post-auth {@code maxFrameLength}) and dribble ~1 MiB, then stall.
 * <p>
 * Defence: {@code Netty5FrameDecoder} checks the announced length against the 64 KiB pre-auth limit as soon as
 * the length field has arrived and closes the connection, so at most 50 x 64 KiB (plus overhead) is ever
 * buffered.
 */
class Attack03PreAuthMemoryTest {

    private static final int CONNECTIONS = 50;
    private static final int ANNOUNCED_CONTENT = 15 * 1024 * 1024;
    private static final int DRIBBLE_BYTES = 1024 * 1024; // 1 MiB the attacker tries to send per connection
    private static final long OVERHEAD = 1024 * 1024;

    @Test
    void preAuthLimitBoundsBufferedMemory() throws Exception {
        // long auth timeout so only the frame limit can close the stalled connections
        var harness = AttackHarness.start(true, 60_000);
        var sockets = new ArrayList<Socket>();
        try {
            long nettyBefore = nettyUsedMemory();
            long heapBefore = heapUsed();

            for (int i = 0; i < CONNECTIONS; i++) {
                var socket = harness.connect();
                sockets.add(socket);
                try {
                    AttackHarness.send(socket,
                            AttackHarness.frameHeaderOnly("de.dataflair.netty5.common.packet.auth.AuthPacket",
                                    ANNOUNCED_CONTENT));
                    socket.getOutputStream().write(new byte[DRIBBLE_BYTES]);
                    socket.getOutputStream().flush();
                } catch (Exception e) {
                    // the server closed the connection while we were still sending
                }
            }
            Thread.sleep(1_500);

            long nettyAfter = nettyUsedMemory();
            long heapAfter = heapUsed();
            long nettyGrowth = nettyAfter - nettyBefore;
            long heapGrowth = heapAfter - heapBefore;
            long preAuthBudget = (long) CONNECTIONS * 64 * 1024;
            int closed = 0;
            for (var socket : sockets) {
                if (closedWithin(socket, 2_000)) {
                    closed++;
                }
            }

            System.out.println("[attack3] connections=" + CONNECTIONS + " closed=" + closed
                    + " netty used before=" + nettyBefore + " after=" + nettyAfter + " growth=" + nettyGrowth
                    + " | heap before=" + heapBefore + " after=" + heapAfter + " growth=" + heapGrowth
                    + " | budget (50x64KiB + 1 MiB)=" + (preAuthBudget + OVERHEAD));

            assertEquals(CONNECTIONS, closed, "every oversized pre-auth frame must close its connection");
            assertTrue(nettyGrowth <= preAuthBudget + OVERHEAD,
                    "server buffered " + nettyGrowth + " bytes pre-auth, more than " + (preAuthBudget + OVERHEAD));
            assertEquals(0, harness.server().connections().size());

            // a legitimate client is still served
            try (var legit = harness.connect()) {
                AttackHarness.send(legit, AttackHarness.authFrame("legit", AttackHarness.TOKEN));
                harness.awaitAuthenticated("legit", 5_000);
                long start = System.nanoTime();
                AttackHarness.send(legit, AttackHarness.frame(TestPacket.class.getName(),
                        AttackHarness.testPacketContent("ping", 1)));
                boolean echoed = AttackHarness.readFrameOf(legit, TestPacket.class.getName(), 5_000);
                long latencyMs = (System.nanoTime() - start) / 1_000_000;
                System.out.println("[attack3] legit client echo received=" + echoed + " latencyMs=" + latencyMs);
                assertTrue(echoed, "legit client should still be serviced");
            }
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

    private static boolean closedWithin(Socket socket, long millis) {
        try {
            socket.setSoTimeout((int) millis);
            var in = socket.getInputStream();
            return in.read() == -1;
        } catch (java.net.SocketException e) {
            return true; // connection reset: closed by the server
        } catch (Exception e) {
            return false;
        }
    }


    private static long nettyUsedMemory() {
        var alloc = ByteBufAllocator.DEFAULT;
        if (alloc instanceof ByteBufAllocatorMetricProvider provider) {
            var metric = provider.metric();
            return metric.usedDirectMemory() + metric.usedHeapMemory();
        }
        return 0;
    }

    private static long heapUsed() {
        for (int i = 0; i < 3; i++) {
            System.gc();
            try {
                Thread.sleep(30);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }
        return java.lang.management.ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed();
    }
}

package de.dataflair.netty.attack;

import de.dataflair.netty.server.NettyServer;
import de.dataflair.netty.testpackets.TestPacket;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Attack 8 - repeated post-auth error frames.
 * <p>
 * Defence: after {@link NettyServer#DEFAULT_MAX_REJECTED_PACKETS} rejected packets the server closes the
 * connection, and log lines are throttled per channel, so one logged-in peer can neither keep the connection
 * nor flood the log.
 */
class Attack08ExceptionFloodTest {

    private static final int FLOOD = 500;

    @Test
    void repeatedErrorFramesCloseTheConnection() throws Exception {
        var harness = AttackHarness.start(true, 20_000);
        var originalErr = System.err;
        var errBytes = new AtomicLong();
        // slf4j-simple writes to System.err
        System.setErr(new PrintStream(new OutputStream() {
            @Override
            public void write(int b) {
                errBytes.incrementAndGet();
            }

            @Override
            public void write(byte[] b, int off, int len) {
                errBytes.addAndGet(len);
            }
        }, true, StandardCharsets.UTF_8));

        Socket socket = null;
        try {
            socket = harness.connect();
            AttackHarness.send(socket, AttackHarness.authFrame("flooder", AttackHarness.TOKEN));
            harness.awaitAuthenticated("flooder", 5_000);

            // malformed TestPacket: negative string length -> decode throws -> PacketRejectedException
            var bad = new ByteArrayOutputStream();
            new DataOutputStream(bad).writeInt(-1);
            var badFrame = AttackHarness.frame(TestPacket.class.getName(), bad.toByteArray());

            long errBefore = errBytes.get();
            int written = 0;
            try {
                for (int i = 0; i < FLOOD; i++) {
                    AttackHarness.send(socket, badFrame);
                    written++;
                }
            } catch (Exception e) {
                // the server closed the connection
            }
            boolean closed = closedWithin(socket, 3_000);
            long logged = errBytes.get() - errBefore;
            boolean stillConnected = true;
            long deadline = System.currentTimeMillis() + 3_000;
            while (stillConnected && System.currentTimeMillis() < deadline) {
                stillConnected = harness.server().connections().stream()
                        .anyMatch(c -> c.identity().name().equals("flooder"));
                Thread.sleep(20);
            }

            System.setErr(originalErr);
            System.out.println("[attack8] framesWritten=" + written + " errBytesLogged=" + logged
                    + " closed=" + closed + " stillConnected=" + stillConnected);

            assertTrue(closed, "the connection must be closed after too many rejected packets");
            assertFalse(stillConnected, "the flooder must no longer be an authenticated connection");
            assertTrue(logged < 20L * 1024, "log output must be throttled (" + logged + " bytes for "
                    + written + " frames)");
        } finally {
            System.setErr(originalErr);
            if (socket != null) {
                try {
                    socket.close();
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
            long deadline = System.currentTimeMillis() + millis;
            while (System.currentTimeMillis() < deadline) {
                if (in.read() == -1) {
                    return true;
                }
            }
            return false;
        } catch (java.net.SocketException e) {
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}

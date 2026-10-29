package de.dataflair.netty5.attack;

import de.dataflair.netty5.testpackets.TestPacket;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Attack 1 - Pipelining: an {@code AuthPacket} with a wrong token plus a payload packet in the same
 * {@code write()}. The payload must never reach a listener.
 */
class Attack01PipeliningTest {

    @Test
    void payloadPipelinedBehindRejectedAuthIsNeverDelivered() throws Exception {
        var harness = AttackHarness.start();
        var delivered = new AtomicInteger();
        harness.server().packetTransmitter().listen(TestPacket.class, (sender, packet) -> delivered.incrementAndGet());
        try (var socket = harness.connect()) {
            // one write(): wrong-token auth frame immediately followed by a (registered) payload frame
            var wrongAuth = AttackHarness.authFrame("pipeliner", "wrong-token");
            var payload = AttackHarness.frame(TestPacket.class.getName(),
                    AttackHarness.testPacketContent("smuggled", 42));
            var combined = new byte[wrongAuth.length + payload.length];
            System.arraycopy(wrongAuth, 0, combined, 0, wrongAuth.length);
            System.arraycopy(payload, 0, combined, wrongAuth.length, payload.length);
            AttackHarness.send(socket, combined);

            // give the server time to process both frames and close
            var in = socket.getInputStream();
            long deadline = System.currentTimeMillis() + 2_000;
            boolean closed = false;
            while (System.currentTimeMillis() < deadline) {
                if (in.read() == -1) {
                    closed = true;
                    break;
                }
            }
            assertTrue(closed, "connection should be closed after a rejected pre-auth payload");
            assertEquals(0, delivered.get(), "pipelined payload must not be delivered to any listener");
        } finally {
            harness.stop();
        }
    }
}

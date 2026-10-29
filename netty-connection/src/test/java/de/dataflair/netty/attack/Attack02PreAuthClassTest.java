package de.dataflair.netty5.attack;

import de.dataflair.netty5.Netty5ClientChannel;
import de.dataflair.netty5.testpackets.TestPacket;
import org.junit.jupiter.api.Test;

import java.net.Socket;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Attack 2 - a real {@code AuthPacket} subclass, or any other class name, sent before authentication. Only the
 * exact {@code AuthPacket} class name may authenticate.
 */
class Attack02PreAuthClassTest {

    @Test
    void authPacketSubclassCannotAuthenticate() throws Exception {
        var harness = AttackHarness.start();
        try (var socket = harness.connect()) {
            // valid AuthPacket wire content, but announced under the subclass name
            var content = AttackHarness.authContent("evil", UUID.randomUUID(),
                    Map.of("token", AttackHarness.TOKEN));
            AttackHarness.send(socket, AttackHarness.frame(EvilAuthSubclass.class.getName(), content));

            assertTrue(closedWithin(socket, 2_000), "subclass-named auth frame must close the channel");
            // never authenticated
            assertThrows(Exception.class, () -> harness.onAuthenticated("evil").get(500, TimeUnit.MILLISECONDS));
            assertEquals(0, harness.server().connections().size());
        } finally {
            harness.stop();
        }
    }

    @Test
    void registeredNonAuthPacketBeforeAuthIsRejected() throws Exception {
        var harness = AttackHarness.start();
        var delivered = new AtomicInteger();
        harness.server().packetTransmitter().listen(TestPacket.class, (s, p) -> delivered.incrementAndGet());
        try (var socket = harness.connect()) {
            AttackHarness.send(socket, AttackHarness.frame(TestPacket.class.getName(),
                    AttackHarness.testPacketContent("early", 1)));
            assertTrue(closedWithin(socket, 2_000), "non-auth packet before auth must close the channel");
            assertEquals(0, delivered.get());
        } finally {
            harness.stop();
        }
    }

    private static boolean closedWithin(Socket socket, long millis) throws Exception {
        var in = socket.getInputStream();
        long deadline = System.currentTimeMillis() + millis;
        while (System.currentTimeMillis() < deadline) {
            if (in.read() == -1) {
                return true;
            }
        }
        return false;
    }
}

package de.dataflair.netty5.attack;

import de.dataflair.netty5.Netty5ClientChannel;
import de.dataflair.netty5.common.packet.Packet;
import de.dataflair.netty5.testpackets.TestRequestPacket;
import de.dataflair.netty5.testpackets.TestRespondPacket;
import org.junit.jupiter.api.Test;

import java.net.Socket;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Attack 6 - a logged-in client A tries to answer a request the server sent to a different client B, using B's
 * exact query id. Pending requests live on each channel's own transmitter, so A's response is matched only
 * against A's (empty) pending map and can never complete B's request. (The query id is also a 122-bit
 * {@code SecureRandom} UUID, so it cannot be guessed either.)
 */
class Attack06ResponseSpoofTest {

    @Test
    void clientCannotCompleteAnotherClientsPendingRequest() throws Exception {
        var harness = AttackHarness.start(false, 15_000);
        Socket a = null;
        Socket b = null;
        try {
            a = auth(harness, "A");
            b = auth(harness, "B");
            var bChannel = harness.awaitAuthenticated("B", 5_000);

            // server sends a request to B through B's channel transmitter; B (raw socket) ignores it
            var request = new TestRequestPacket("for-B", List.of());
            CompletableFuture<Packet> fB = bChannel.transmitter().queryPacket(request, TestRespondPacket.class);
            UUID bQueryId = request.queryId();
            System.out.println("[attack6] server->B queryId=" + bQueryId);

            // A sends a RespondPacket carrying B's exact query id
            AttackHarness.send(a, AttackHarness.frame(TestRespondPacket.class.getName(),
                    AttackHarness.respondContent(bQueryId, "spoofed-by-A")));

            Thread.sleep(1_500);
            boolean completed = fB.isDone() && !fB.isCompletedExceptionally();
            System.out.println("[attack6] B's future completed by A's spoof=" + completed
                    + " (done=" + fB.isDone() + ")");
            assertFalse(completed, "A must not be able to complete a request routed to B");
        } finally {
            close(a);
            close(b);
            harness.stop();
        }
    }

    private static Socket auth(AttackHarness harness, String name) throws Exception {
        var socket = harness.connect();
        AttackHarness.send(socket, AttackHarness.authFrame(name, AttackHarness.TOKEN));
        Netty5ClientChannel channel = harness.awaitAuthenticated(name, 5_000);
        if (channel == null) {
            throw new IllegalStateException("not authenticated: " + name);
        }
        return socket;
    }

    private static void close(Socket s) {
        if (s != null) {
            try {
                s.close();
            } catch (Exception ignored) {
            }
        }
    }
}

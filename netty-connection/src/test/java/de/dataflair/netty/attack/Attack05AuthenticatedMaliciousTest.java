package de.dataflair.netty.attack;

import de.dataflair.netty.common.codec.CodecBuffer;
import de.dataflair.netty.testpackets.TestPacket;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Attack 5 - an authenticated peer sends malicious payloads: huge declared list length in a small frame, negative
 * lengths, broken UTF-8 and a JSON bomb. The server must stay up; a post-auth decode error must not crash it.
 */
class Attack05AuthenticatedMaliciousTest {

    /**
     * Huge declared list count with a tiny frame. {@code readByteList} reads the count and then one byte per
     * element, so a 2^31-1 count with only 4 payload bytes exhausts the buffer immediately: no 2-billion-element
     * allocation happens. The count is bounded by the frame size (<= 16 MiB), but note there is no up-front
     * validation of the count and a byte/boolean list amplifies wire bytes into an ArrayList by ~8x.
     */
    @Test
    void hugeDeclaredListLengthInTinyFrameDoesNotAllocate() throws Exception {
        var harness = AttackHarness.start(true, 15_000);
        try (var socket = auth(harness, "m")) {
            var content = new ByteArrayOutputStream();
            var data = new DataOutputStream(content);
            data.writeInt(Integer.MAX_VALUE); // declared element count
            data.write(new byte[]{1, 2, 3, 4}); // only 4 actual bytes
            long before = usedHeap();
            AttackHarness.send(socket, AttackHarness.frame(ListBombPacket.class.getName(), content.toByteArray()));
            Thread.sleep(500);
            long after = usedHeap();
            System.out.println("[attack5-list] heap growth=" + (after - before));
            assertTrue(after - before < 64L * 1024 * 1024, "no huge allocation from a tiny frame");
            assertTrue(stillAlive(harness, socket), "connection survives a rejected oversized-list packet");
        } finally {
            harness.stop();
        }
    }

    @Test
    void negativeStringLengthIsRejectedWithoutCrash() throws Exception {
        var harness = AttackHarness.start(true, 15_000);
        try (var socket = auth(harness, "m")) {
            var content = new ByteArrayOutputStream();
            new DataOutputStream(content).writeInt(-1); // negative string length for TestPacket.message
            AttackHarness.send(socket, AttackHarness.frame(TestPacket.class.getName(), content.toByteArray()));
            Thread.sleep(300);
            assertTrue(stillAlive(harness, socket), "connection survives a negative-length packet");
        } finally {
            harness.stop();
        }
    }

    @Test
    void brokenUtf8IsDecodedWithoutCrash() throws Exception {
        var harness = AttackHarness.start(true, 15_000);
        var delivered = new AtomicReference<String>();
        harness.server().packetTransmitter().listen(TestPacket.class, (s, p) -> {
            if (p.number() == 777) {
                delivered.set(p.message());
            }
        });
        try (var socket = auth(harness, "m")) {
            var content = new ByteArrayOutputStream();
            var data = new DataOutputStream(content);
            data.writeInt(3);
            data.write(new byte[]{(byte) 0xFF, (byte) 0xFE, (byte) 0xC0}); // invalid UTF-8
            data.writeInt(777);
            AttackHarness.send(socket, AttackHarness.frame(TestPacket.class.getName(), content.toByteArray()));
            long deadline = System.currentTimeMillis() + 2_000;
            while (delivered.get() == null && System.currentTimeMillis() < deadline) {
                Thread.sleep(20);
            }
            System.out.println("[attack5-utf8] delivered message=" + delivered.get());
            assertNotNull(delivered.get(), "broken UTF-8 is decoded with replacement chars, not a crash");
        } finally {
            harness.stop();
        }
    }

    /**
     * JSON bomb characterisation. The library's JSON packets store the raw string and never parse it during
     * decode, so a socket peer cannot trigger this via the demo/library packets. {@code CodecBuffer.readJsonObject}
     * parses with Gson; Gson 2.15 enforces a nesting limit and rejects a deeply nested document with a
     * {@link com.google.gson.JsonSyntaxException} (a {@link RuntimeException}), which {@code PacketFactory}'s
     * {@code catch (ReflectiveOperationException | RuntimeException)} would catch. So this is bounded, not a
     * StackOverflow; a custom app packet parsing huge-but-shallow JSON would still pay the allocation cost.
     */
    @Test
    void deeplyNestedJsonIsRejectedByGsonNestingLimit() {
        var nested = "[".repeat(100_000) + "]".repeat(100_000);
        var buffer = new CodecBuffer(Unpooled.buffer());
        buffer.writeString(nested);
        Throwable thrown = null;
        try {
            buffer.readJsonObject();
        } catch (Throwable t) {
            thrown = t;
        }
        System.out.println("[attack5-json] thrown=" + (thrown == null ? "none" : thrown.getClass().getName()));
        assertNotNull(thrown, "deeply nested JSON should fail");
        // the important property: it is a RuntimeException (catchable by PacketFactory), not an uncaught Error
        assertTrue(thrown instanceof RuntimeException && !(thrown instanceof StackOverflowError),
                "expected a catchable RuntimeException, observed " + thrown.getClass().getName());
        assertFalse(thrown instanceof java.io.IOException);
    }

    // ---- helpers --------------------------------------------------------

    private static Socket auth(AttackHarness harness, String name) throws Exception {
        var socket = harness.connect();
        AttackHarness.send(socket, AttackHarness.authFrame(name, AttackHarness.TOKEN));
        harness.awaitAuthenticated(name, 5_000);
        return socket;
    }

    /** Sends a valid TestPacket and checks the echo comes back, proving the channel is still usable. */
    private static boolean stillAlive(AttackHarness harness, Socket socket) {
        try {
            AttackHarness.send(socket, AttackHarness.frame(TestPacket.class.getName(),
                    AttackHarness.testPacketContent("alive", 1)));
            return AttackHarness.readFrameOf(socket, TestPacket.class.getName(), 3_000);
        } catch (Exception e) {
            return false;
        }
    }

    private static long usedHeap() {
        System.gc();
        try {
            Thread.sleep(50);
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
        return java.lang.management.ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed();
    }
}

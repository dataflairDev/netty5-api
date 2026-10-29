package de.dataflair.netty.attack;

import de.dataflair.netty.NettyClientChannel;
import de.dataflair.netty.testpackets.TestPacket;
import org.junit.jupiter.api.Test;

import java.net.Socket;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Attack 7 - an authenticated client never reads, but keeps provoking server responses (the echo server echoes
 * every TestPacket).
 * <p>
 * Defence: when the outbound buffer crosses the high water mark (64 KiB) the server stops reading from that
 * client ({@code autoRead=false}), so the attacker's requests stay in the TCP buffers and the outbound buffer
 * stays near the water mark. If the application keeps writing on its own, the hard limit (default 8 MiB) closes
 * the connection.
 */
class Attack07SlowReaderBackpressureTest {

    @Test
    void serverStopsReadingFromANonReadingClient() throws Exception {
        var harness = AttackHarness.start(true, 20_000);
        Socket socket = null;
        Thread writer = null;
        try {
            socket = harness.connect();
            AttackHarness.send(socket, AttackHarness.authFrame("slow", AttackHarness.TOKEN));
            NettyClientChannel serverSide = harness.awaitAuthenticated("slow", 5_000);
            var nettyChannel = serverSide.channel();

            var payload = "x".repeat(8 * 1024); // ~8 KiB per echoed packet
            var sent = new AtomicInteger();
            var attackerSocket = socket;
            // the client never reads its socket; it only keeps writing requests the server would echo back.
            // Once the server stops reading, the writes block, so they run on their own thread.
            writer = new Thread(() -> {
                try {
                    for (int i = 0; i < 6_000; i++) {
                        AttackHarness.send(attackerSocket, AttackHarness.frame(TestPacket.class.getName(),
                                AttackHarness.testPacketContent(payload, i)));
                        sent.incrementAndGet();
                    }
                } catch (Exception e) {
                    // socket closed at the end of the test
                }
            }, "attack7-writer");
            writer.setDaemon(true);
            writer.start();

            Thread.sleep(3_000);
            long pending = pendingOutbound(nettyChannel);
            boolean autoRead = nettyChannel.config().isAutoRead();
            int sentAfterWait = sent.get();
            Thread.sleep(1_000);
            long pendingLater = pendingOutbound(nettyChannel);
            int sentLater = sent.get();

            System.out.println("[attack7] sent=" + sentAfterWait + "->" + sentLater
                    + " isWritable=" + nettyChannel.isWritable() + " autoRead=" + autoRead
                    + " highWaterMark=" + nettyChannel.config().getWriteBufferHighWaterMark()
                    + " pendingOutbound=" + pending + "->" + pendingLater
                    + " open=" + nettyChannel.isOpen());

            if (nettyChannel.isOpen()) {
                assertFalse(autoRead, "the server must stop reading from a client that does not read");
                assertTrue(pendingLater <= 2L * 1024 * 1024,
                        "outbound buffer must stay bounded (" + pendingLater + " bytes pending)");
                assertTrue(sentLater < 6_000, "the attacker's writes must be blocked by TCP backpressure");
            }
        } finally {
            if (socket != null) {
                try {
                    socket.close();
                } catch (Exception ignored) {
                }
            }
            if (writer != null) {
                writer.join(5_000);
            }
            harness.stop();
        }
    }

    @Test
    void outboundBufferAboveHardLimitClosesTheConnection() throws Exception {
        // the application writes 4 MiB per received packet on its own, ignoring isWritable()
        var harness = AttackHarness.start(false, 20_000, server -> server.maxOutboundBufferBytes(1024 * 1024));
        harness.server().packetTransmitter().listen(TestPacket.class, (sender, packet) -> {
            var big = "y".repeat(8 * 1024);
            for (int i = 0; i < 512; i++) {
                sender.sendPacket(new TestPacket(big, i));
            }
        });
        try (var socket = harness.connect()) {
            AttackHarness.send(socket, AttackHarness.authFrame("flooded", AttackHarness.TOKEN));
            var serverSide = harness.awaitAuthenticated("flooded", 5_000);
            AttackHarness.send(socket, AttackHarness.frame(TestPacket.class.getName(),
                    AttackHarness.testPacketContent("go", 1)));
            serverSide.channel().closeFuture().await(5_000);
            System.out.println("[attack7-hard-limit] open=" + serverSide.channel().isOpen());
            assertFalse(serverSide.channel().isOpen(), "exceeding the outbound hard limit must close the channel");
        } finally {
            harness.stop();
        }
    }

    private static long pendingOutbound(io.netty.channel.Channel channel) {
        var buffer = channel.unsafe().outboundBuffer();
        return buffer == null ? 0 : buffer.totalPendingWriteBytes();
    }
}

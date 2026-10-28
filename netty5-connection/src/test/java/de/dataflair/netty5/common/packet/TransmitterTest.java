package de.dataflair.netty5.common.packet;

import de.dataflair.netty5.Netty5ChannelUtils;
import de.dataflair.netty5.client.Netty5ClientPacketTransmitter;
import de.dataflair.netty5.common.packet.json.JsonRespondPacket;
import de.dataflair.netty5.server.Netty5ServerPacketTransmitter;
import de.dataflair.netty5.testpackets.TestRequestPacket;
import de.dataflair.netty5.testpackets.TestRespondPacket;
import io.netty.channel.EventLoopGroup;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;

class TransmitterTest {
    private EventLoopGroup group;
    private BlockingQueue<Packet> sent;
    private Netty5ClientPacketTransmitter transmitter;

    @BeforeEach
    void setUp() {
        group = Netty5ChannelUtils.createEventLoopGroup(1);
        sent = new LinkedBlockingQueue<>();
        transmitter = new Netty5ClientPacketTransmitter(group, sent::add);
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        group.shutdownGracefully(0, 1, TimeUnit.SECONDS).await(5, TimeUnit.SECONDS);
    }

    private static TestRespondPacket responseFor(Packet request, String value) {
        var response = new TestRespondPacket(value);
        response.queryId(((RequestPacket) request).queryId());
        return response;
    }

    /**
     * Fix 11: concurrent registrations are not lost.
     */
    @Test
    void concurrentListenRegistrationsAreKept() throws Exception {
        int threads = 16;
        int perThread = 200;
        var pool = Executors.newFixedThreadPool(threads);
        var start = new CountDownLatch(1);
        var futures = new ArrayList<java.util.concurrent.Future<?>>();
        for (int t = 0; t < threads; t++) {
            int thread = t;
            futures.add(pool.submit(() -> {
                start.await();
                for (int i = 0; i < perThread; i++) {
                    transmitter.listen(TestRespondPacket.class, "l-" + thread + "-" + i, (c, p) -> {
                    });
                    transmitter.listenQuery(TestRequestPacket.class, "q-" + thread + "-" + i, r -> null);
                }
                return null;
            }));
        }
        start.countDown();
        for (var future : futures) {
            future.get(10, TimeUnit.SECONDS);
        }
        pool.shutdownNow();
        assertEquals(threads * perThread, transmitter.listener().get(TestRespondPacket.class).size());
        assertEquals(threads * perThread, transmitter.responders().get(TestRequestPacket.class).size());
    }

    /**
     * Fix 10: responders are unregistered by their request class.
     */
    @Test
    void unregisterResponderUsesRequestClass() {
        transmitter.listenQuery(TestRequestPacket.class, "key", r -> new TestRespondPacket("x"));
        assertEquals(1, transmitter.responders().get(TestRequestPacket.class).size());
        transmitter.unregisterResponder(TestRequestPacket.class, "key");
        assertTrue(transmitter.responders().get(TestRequestPacket.class).isEmpty());

        transmitter.listen(TestRespondPacket.class, "l", (c, p) -> {
        });
        transmitter.unregisterListener(TestRespondPacket.class, "l");
        transmitter.unregisterListener(JsonRespondPacket.class, "unknown");
        assertTrue(transmitter.listener().get(TestRespondPacket.class).isEmpty());
        assertNull(transmitter.listener().get(JsonRespondPacket.class));
    }

    /**
     * Fix 21: random listener keys use the alphanumeric alphabet.
     */
    @Test
    void generatedKeysAreAlphanumeric() {
        for (int i = 0; i < 200; i++) {
            transmitter.listen(TestRespondPacket.class, (c, p) -> {
            });
        }
        var keys = transmitter.listener().get(TestRespondPacket.class).keySet();
        assertEquals(200, keys.size());
        keys.forEach(key -> assertTrue(key.matches("[A-Za-z0-9]{10}"), key));
    }

    /**
     * Fix 12: a response of the wrong type fails the future instead of being dropped.
     */
    @Test
    void wrongResponseTypeFailsFuture() throws Exception {
        var future = transmitter.queryPacket(new TestRequestPacket("t", List.of()), TestRespondPacket.class);
        var request = sent.poll(5, TimeUnit.SECONDS);
        assertNotNull(request);
        var wrong = new JsonRespondPacket("x");
        wrong.queryId(((RequestPacket) request).queryId());
        transmitter.call(wrong, null);

        var error = assertThrows(ExecutionException.class, () -> future.get(5, TimeUnit.SECONDS));
        assertInstanceOf(ClassCastException.class, error.getCause());
        assertTrue(transmitter.pendingRequests().isEmpty());
    }

    @Test
    void asyncQueryCompletesAndCallbackRuns() throws Exception {
        var callback = new CompletableFuture<TestRespondPacket>();
        transmitter.queryPacket(new TestRequestPacket("t", List.of()), TestRespondPacket.class, callback::complete);
        transmitter.call(responseFor(sent.poll(5, TimeUnit.SECONDS), "done"), null);
        assertEquals("done", callback.get(5, TimeUnit.SECONDS).value());
    }

    /**
     * Fix 9: blocking requests time out instead of busy waiting forever and clean up.
     */
    @Test
    void directRequestTimesOut() {
        transmitter.requestTimeoutMillis(200);
        long start = System.nanoTime();
        assertNull(transmitter.queryPacketDirect(new TestRequestPacket("t", List.of()), TestRespondPacket.class));
        long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        assertTrue(elapsedMillis < 3_000, "took " + elapsedMillis + " ms");
        assertTrue(transmitter.pendingRequests().isEmpty());

        var asyncFuture = transmitter.queryPacket(new TestRequestPacket("t", List.of()), TestRespondPacket.class);
        var error = assertThrows(ExecutionException.class, () -> asyncFuture.get(5, TimeUnit.SECONDS));
        assertInstanceOf(TimeoutException.class, error.getCause());
    }

    /**
     * Fix 9: many concurrent blocking requests are each answered with their own response.
     */
    @Test
    void concurrentDirectRequests() throws Exception {
        var responder = Executors.newSingleThreadExecutor();
        responder.submit(() -> {
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    var request = (TestRequestPacket) sent.take();
                    transmitter.call(responseFor(request, "re:" + request.queryId()), null);
                }
            } catch (InterruptedException ignored) {
            }
        });
        int clients = 32;
        var pool = Executors.newFixedThreadPool(clients);
        var results = new ArrayList<java.util.concurrent.Future<Boolean>>();
        for (int i = 0; i < clients; i++) {
            results.add(pool.submit(() -> {
                var request = new TestRequestPacket("t", List.of());
                var response = transmitter.queryPacketDirect(request, TestRespondPacket.class);
                return response != null && response.value().equals("re:" + request.queryId());
            }));
        }
        for (var result : results) {
            assertTrue(result.get(10, TimeUnit.SECONDS));
        }
        pool.shutdownNow();
        responder.shutdownNow();
        assertTrue(transmitter.pendingRequests().isEmpty());
    }

    /**
     * Fix 9: a blocking request from the event loop fails immediately.
     */
    @Test
    void directRequestOnEventLoopFails() {
        var error = assertThrows(ExecutionException.class, () -> group.next().submit(
                () -> transmitter.queryPacketDirect(new TestRequestPacket("t", List.of()), TestRespondPacket.class)
        ).get(5, TimeUnit.SECONDS));
        assertInstanceOf(IllegalStateException.class, error.getCause());
        assertTrue(sent.isEmpty(), "nothing must be sent");
    }

    /**
     * Fix 18: the server-wide transmitter rejects queries explicitly.
     */
    @Test
    void serverTransmitterQueryIsUnsupported() {
        var server = new Netty5ServerPacketTransmitter(group, p -> {
        }, (a, b, c) -> {
        });
        var request = new TestRequestPacket("t", List.of());
        assertThrows(UnsupportedOperationException.class, () -> server.queryPacket(request, TestRespondPacket.class));
        assertThrows(UnsupportedOperationException.class, () -> server.queryPacketDirect(request, TestRespondPacket.class));
        assertThrows(UnsupportedOperationException.class, () -> server.queryPacket(request, TestRespondPacket.class, p -> {
        }));
    }
}

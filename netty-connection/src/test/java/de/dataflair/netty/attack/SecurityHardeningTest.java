package de.dataflair.netty.attack;

import de.dataflair.netty.NettyClientChannel;
import de.dataflair.netty.client.NettyClient;
import de.dataflair.netty.common.codec.PacketRegistry;
import de.dataflair.netty.common.packet.auth.AuthPacket;
import de.dataflair.netty.filter.AsyncConnectionFilter;
import de.dataflair.netty.filter.ConnectionFilter;
import de.dataflair.netty.filter.ConnectionFilters;
import de.dataflair.netty.server.IdentityPolicy;
import de.dataflair.netty.server.NettyServer;
import de.dataflair.netty.testpackets.TestPacket;
import de.dataflair.netty.testpackets.TestRespondPacket;
import io.netty.handler.ssl.SslContextBuilder;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.net.ssl.KeyManagerFactory;
import java.io.File;
import java.io.FileInputStream;
import java.net.Socket;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end tests of the server side defences (fail-closed start, identity policy, connection limits,
 * authentication throttling, asynchronous filters, duplicate auth packets, unsolicited responses, idle timeout
 * and TLS).
 */
class SecurityHardeningTest {

    // ---- F1 fail-closed ----------------------------------------------------

    @Test
    void serverWithoutConnectionFilterRefusesToStart() throws Exception {
        var server = new NettyServer("127.0.0.1", 0);
        try {
            var error = assertThrows(IllegalStateException.class, server::initialize);
            assertTrue(error.getMessage().contains("allowAnonymous"));
            assertFalse(server.connectionFuture().isDone(), "nothing must be bound");
        } finally {
            stop(server);
        }
    }

    @Test
    void serverWithAllowAnonymousStarts() throws Exception {
        var server = new NettyServer("127.0.0.1", 0);
        try {
            server.allowAnonymous(true);
            server.initialize();
            server.connectionFuture().get(5, TimeUnit.SECONDS);
            assertTrue(server.localAddress() != null);
        } finally {
            stop(server);
        }
    }

    // ---- F2 identity ---------------------------------------------------------

    @Test
    void duplicateIdentityIsRejectedByDefault() throws Exception {
        var harness = AttackHarness.start();
        try (var first = harness.connect(); var second = harness.connect(); var third = harness.connect()) {
            var uuid = UUID.randomUUID();
            AttackHarness.send(first, authFrame("alice", uuid));
            harness.awaitAuthenticated("alice", 5_000);
            // same name (other case), other UUID
            AttackHarness.send(second, authFrame("ALICE", UUID.randomUUID()));
            assertTrue(closedWithin(second, 3_000), "duplicate name must be rejected");
            // other name, same UUID: the server derives the UUID from the name, so this is no conflict
            AttackHarness.send(third, authFrame("bob", uuid));
            harness.awaitAuthenticated("bob", 5_000);
            assertEquals(2, harness.server().connections().size());
            assertFalse(closedWithin(first, 300), "the first connection stays");
        } finally {
            harness.stop();
        }
    }

    @Test
    void replaceOldPolicyClosesTheExistingConnection() throws Exception {
        var harness = AttackHarness.start(false, 10_000, server -> server.identityPolicy(IdentityPolicy.REPLACE_OLD));
        try (var first = harness.connect(); var second = harness.connect()) {
            AttackHarness.send(first, authFrame("carol", UUID.randomUUID()));
            harness.awaitAuthenticated("carol", 5_000);
            AttackHarness.send(second, authFrame("carol", UUID.randomUUID()));
            assertTrue(closedWithin(first, 3_000), "the old connection must be closed");
            awaitTrue(() -> harness.server().connections().size() == 1
                    && harness.server().connections().getFirst().channel().isActive(), "one active connection");
        } finally {
            harness.stop();
        }
    }

    // ---- F4 limits -------------------------------------------------------------

    @Test
    void connectionsPerAddressAreLimited() throws Exception {
        var harness = AttackHarness.start(false, 10_000, server -> server.maxConnectionsPerAddress(3));
        var sockets = new ArrayList<Socket>();
        try {
            for (int i = 0; i < 3; i++) {
                sockets.add(harness.connect());
            }
            Thread.sleep(200);
            var fourth = harness.connect();
            sockets.add(fourth);
            assertTrue(closedWithin(fourth, 2_000), "the fourth connection from one address must be closed");
            for (int i = 0; i < 3; i++) {
                assertFalse(closedWithin(sockets.get(i), 100), "the first three stay open");
            }
            // a closed connection frees its slot
            sockets.getFirst().close();
            Thread.sleep(300);
            var fifth = harness.connect();
            sockets.add(fifth);
            AttackHarness.send(fifth, AttackHarness.authFrame("late", AttackHarness.TOKEN));
            harness.awaitAuthenticated("late", 5_000);
        } finally {
            for (var socket : sockets) {
                socket.close();
            }
            harness.stop();
        }
    }

    @Test
    void globalConnectionLimit() throws Exception {
        var harness = AttackHarness.start(false, 10_000, server -> server.maxConnections(2));
        try (var a = harness.connect(); var b = harness.connect()) {
            Thread.sleep(200);
            try (var c = harness.connect()) {
                assertTrue(closedWithin(c, 2_000), "the third connection must be closed");
            }
            assertFalse(closedWithin(a, 100));
            assertFalse(closedWithin(b, 100));
        } finally {
            harness.stop();
        }
    }

    @Test
    void failedAuthenticationsAreThrottledPerAddress() throws Exception {
        var filterCalls = new AtomicInteger();
        var harness = AttackHarness.start(false, 10_000, server -> {
            server.maxAuthFailuresPerMinute(3);
            server.addFilter(new ConnectionFilter() {
                @Override
                public boolean evaluateFilter(NettyClientChannel.@NotNull AuthType object) {
                    filterCalls.incrementAndGet();
                    return true;
                }
            });
        });
        try {
            for (int i = 0; i < 3; i++) {
                try (var socket = harness.connect()) {
                    AttackHarness.send(socket, AttackHarness.authFrame("guess" + i, "wrong"));
                    assertTrue(closedWithin(socket, 2_000));
                }
            }
            // the address is blocked now: even the right token is refused, the connection is closed at once
            try (var socket = harness.connect()) {
                assertTrue(closedWithin(socket, 2_000), "blocked address must be closed right away");
            }
            assertThrows(Exception.class, () -> harness.awaitAuthenticated("legit", 300));
        } finally {
            harness.stop();
        }
    }

    // ---- F9 async filter, F13 single evaluation -----------------------------------

    @Test
    void asyncFilterDecidesAndPipelinedPacketsFollowAfterwards() throws Exception {
        var executor = Executors.newSingleThreadScheduledExecutor();
        var harness = AttackHarness.start(true, 10_000, server -> server.addFilter(new AsyncConnectionFilter() {
            @Override
            public @NotNull CompletionStage<Boolean> evaluateAsync(NettyClientChannel.@NotNull AuthType auth) {
                var result = new CompletableFuture<Boolean>();
                executor.schedule(() -> result.complete(
                        !"denied".equals(auth.clientChannel().identity().name())), 200, TimeUnit.MILLISECONDS);
                return result;
            }
        }));
        try (var accepted = harness.connect(); var denied = harness.connect()) {
            // auth and a payload in one write: the payload is decoded only after the filter accepted
            AttackHarness.send(accepted, concat(AttackHarness.authFrame("async-ok", AttackHarness.TOKEN),
                    AttackHarness.frame(TestPacket.class.getName(), AttackHarness.testPacketContent("early", 7))));
            assertTrue(AttackHarness.readFrameOf(accepted, TestPacket.class.getName(), 3_000), "pipelined payload must be echoed after acceptance");

            AttackHarness.send(denied, AttackHarness.authFrame("denied", AttackHarness.TOKEN));
            assertTrue(closedWithin(denied, 3_000), "async rejection must close the connection");
        } finally {
            harness.stop();
            executor.shutdownNow();
        }
    }

    @Test
    void severalAuthPacketsInOneWriteRunTheFilterOnce() throws Exception {
        var calls = new AtomicInteger();
        var harness = AttackHarness.start(false, 10_000, server -> server.addFilter(new ConnectionFilter() {
            @Override
            public boolean evaluateFilter(NettyClientChannel.@NotNull AuthType object) {
                calls.incrementAndGet();
                return true;
            }
        }));
        try (var wrong = harness.connect(); var right = harness.connect()) {
            var frame = AttackHarness.authFrame("multi", "wrong-token");
            AttackHarness.send(wrong, concat(frame, frame, frame, frame));
            assertTrue(closedWithin(wrong, 2_000));
            // the token filter of the harness rejected the first packet; nothing after it was evaluated
            assertEquals(0, calls.get());

            var good = AttackHarness.authFrame("multi-ok", AttackHarness.TOKEN);
            AttackHarness.send(right, concat(good, good, good));
            harness.awaitAuthenticated("multi-ok", 3_000);
            Thread.sleep(300);
            assertEquals(1, calls.get(), "the filter runs for the first auth packet only");
        } finally {
            harness.stop();
        }
    }

    // ---- F14 unsolicited responses ------------------------------------------------

    @Test
    void unsolicitedResponseIsNotPassedToListeners() throws Exception {
        var harness = AttackHarness.start();
        var delivered = new AtomicInteger();
        harness.server().packetTransmitter().listen(TestRespondPacket.class, (sender, packet) -> delivered.incrementAndGet());
        try (var socket = harness.connect()) {
            AttackHarness.send(socket, AttackHarness.authFrame("responder", AttackHarness.TOKEN));
            harness.awaitAuthenticated("responder", 5_000);
            AttackHarness.send(socket, AttackHarness.frame(TestRespondPacket.class.getName(),
                    AttackHarness.respondContent(UUID.randomUUID(), "unasked")));
            Thread.sleep(500);
            assertEquals(0, delivered.get());
        } finally {
            harness.stop();
        }
    }

    // ---- F16 idle timeout -------------------------------------------------------

    @Test
    void idleAuthenticatedConnectionIsClosed() throws Exception {
        var harness = AttackHarness.start(false, 10_000, server -> server.idleTimeoutMillis(500));
        try (var socket = harness.connect()) {
            AttackHarness.send(socket, AttackHarness.authFrame("sleepy", AttackHarness.TOKEN));
            harness.awaitAuthenticated("sleepy", 5_000);
            assertTrue(closedWithin(socket, 3_000), "idle connection must be closed");
        } finally {
            harness.stop();
        }
    }

    @Test
    void negativeIdleTimeoutIsRejected() throws Exception {
        var server = new NettyServer("127.0.0.1", 0);
        try {
            server.allowAnonymous(true);
            server.idleTimeoutMillis(-1);
            assertThrows(IllegalArgumentException.class, server::initialize);
        } finally {
            stop(server);
        }
    }

    // ---- F12 --------------------------------------------------------------------

    @Test
    void tokenEqualsComparesInConstantTime() {
        assertTrue(ConnectionFilters.tokenEquals("secret", "secret"));
        assertFalse(ConnectionFilters.tokenEquals("secret", "secreT"));
        assertFalse(ConnectionFilters.tokenEquals("secret", "secret-longer"));
        assertFalse(ConnectionFilters.tokenEquals("secret", null));
        assertFalse(ConnectionFilters.tokenEquals(null, null));
        assertTrue(ConnectionFilters.constantTimeEquals(new byte[]{1, 2}, new byte[]{1, 2}));
        assertFalse(ConnectionFilters.constantTimeEquals(new byte[]{1, 2}, null));
    }

    // ---- F3 TLS -----------------------------------------------------------------

    @Test
    void tlsWithSelfSignedCertificate(@TempDir Path directory) throws Exception {
        var keyStoreFile = directory.resolve("server.p12").toFile();
        generateKeyStore(keyStoreFile);
        var keyStore = KeyStore.getInstance("PKCS12");
        try (var in = new FileInputStream(keyStoreFile)) {
            keyStore.load(in, "changeit".toCharArray());
        }
        var keyManagers = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keyManagers.init(keyStore, "changeit".toCharArray());
        var certificate = (X509Certificate) keyStore.getCertificate("server");

        var authenticated = new CopyOnWriteArrayList<String>();
        var server = new NettyServer("127.0.0.1", 0);
        server.sslContext(SslContextBuilder.forServer(keyManagers).build());
        server.packetRegistry(new PacketRegistry().register(TestPacket.class));
        server.addFilter(new ConnectionFilter() {
            @Override
            public boolean evaluateFilter(NettyClientChannel.@NotNull AuthType auth) {
                return ConnectionFilters.tokenEquals("tls-token", auth.authProperty().get("token"));
            }
        });
        server.addAction(new de.dataflair.netty.actions.ConnectionAction(channel ->
                authenticated.add(channel.identity().name()),
                de.dataflair.netty.actions.ConnectionAction.State.CLIENT_AUTHENTICATED));
        var received = new CompletableFuture<String>();
        server.packetTransmitter().listen(TestPacket.class, (sender, packet) -> received.complete(packet.message()));
        NettyClient client = null;
        try {
            server.initialize();
            server.connectionFuture().get(5, TimeUnit.SECONDS);
            int port = server.localAddress().getPort();

            client = new NettyClient("localhost", port, new NettyClientChannel.Identity("tls-client",
                    UUID.randomUUID()), Map.of("token", "tls-token"));
            client.sslContext(SslContextBuilder.forClient().trustManager(certificate).build());
            client.packetRegistry(new PacketRegistry().register(TestPacket.class));
            client.initialize();
            client.connectionFuture().get(5, TimeUnit.SECONDS);
            client.thisChannel().sendPacket(new TestPacket("over tls", 1));
            assertEquals("over tls", received.get(5, TimeUnit.SECONDS));
            assertEquals(java.util.List.of("tls-client"), authenticated);

            // a plain TCP peer cannot authenticate against the TLS server
            try (var plain = new Socket("127.0.0.1", port)) {
                plain.getOutputStream().write(AttackHarness.authFrame("plain", "tls-token"));
                assertTrue(closedWithin(plain, 3_000), "plain auth packet must not pass the TLS handler");
            }
            assertFalse(authenticated.contains("plain"));
        } finally {
            if (client != null) {
                client.shutdownGracefully(0, 1, TimeUnit.SECONDS).await(5, TimeUnit.SECONDS);
            }
            stop(server);
        }
    }

    // ---- helpers ----------------------------------------------------------------

    static void generateKeyStore(File file) throws Exception {
        var keytool = Path.of(System.getProperty("java.home"), "bin",
                System.getProperty("os.name").toLowerCase().contains("win") ? "keytool.exe" : "keytool");
        var process = new ProcessBuilder(keytool.toString(), "-genkeypair", "-alias", "server",
                "-keyalg", "EC", "-groupname", "secp256r1", "-dname", "CN=localhost",
                "-ext", "SAN=dns:localhost,ip:127.0.0.1", "-validity", "2",
                "-storetype", "PKCS12", "-keystore", file.getAbsolutePath(),
                "-storepass", "changeit", "-keypass", "changeit")
                .redirectErrorStream(true)
                .start();
        var output = new String(process.getInputStream().readAllBytes());
        assertTrue(process.waitFor(60, TimeUnit.SECONDS), "keytool timed out");
        assertEquals(0, process.exitValue(), "keytool failed: " + output);
    }

    private static byte[] authFrame(String name, UUID uuid) {
        return AttackHarness.frame(AuthPacket.class.getName(),
                AttackHarness.authContent(name, uuid, Map.of("token", AttackHarness.TOKEN)));
    }

    private static byte[] concat(byte[]... parts) {
        var length = 0;
        for (var part : parts) {
            length += part.length;
        }
        var result = new byte[length];
        var offset = 0;
        for (var part : parts) {
            System.arraycopy(part, 0, result, offset, part.length);
            offset += part.length;
        }
        return result;
    }

    static boolean closedWithin(Socket socket, long millis) {
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
        } catch (java.net.SocketTimeoutException e) {
            return false;
        } catch (Exception e) {
            return true; // reset by the server
        }
    }


    private static void awaitTrue(java.util.function.BooleanSupplier condition, String what) throws Exception {
        long deadline = System.currentTimeMillis() + 3_000;
        while (!condition.getAsBoolean()) {
            if (System.currentTimeMillis() > deadline) {
                throw new AssertionError("timeout: " + what);
            }
            Thread.sleep(20);
        }
    }

    private static void stop(NettyServer server) throws InterruptedException {
        server.shutdownGracefully(0, 1, TimeUnit.SECONDS).await(5, TimeUnit.SECONDS);
        server.workerGroup().terminationFuture().await(5, TimeUnit.SECONDS);
    }
}

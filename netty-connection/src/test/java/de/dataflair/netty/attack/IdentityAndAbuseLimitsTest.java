package de.dataflair.netty.attack;

import de.dataflair.netty.NettyClientChannel;
import de.dataflair.netty.NettyIdentities;
import de.dataflair.netty.common.packet.auth.AuthPacket;
import de.dataflair.netty.filter.AsyncConnectionFilter;
import de.dataflair.netty.server.IdentityPolicy;
import io.netty.handler.ssl.SslContextBuilder;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.net.ssl.KeyManagerFactory;
import java.io.FileInputStream;
import java.net.Socket;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static de.dataflair.netty.attack.SecurityHardeningTest.closedWithin;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Identity binding (derived UUID, canonical names), atomic counting of authentication attempts, pre-auth slots,
 * connection rate per prefix, duplicate auth packets and TLS handshake failures.
 */
class IdentityAndAbuseLimitsTest {

    // ---- H1 UUID bound to the identity ----------------------------------------------

    @Test
    void uuidTakeoverFailsWithReplaceOld() throws Exception {
        var harness = AttackHarness.start(false, 10_000, server -> server.identityPolicy(IdentityPolicy.REPLACE_OLD));
        try (var alice = harness.connect(); var bob = harness.connect()) {
            var aliceUuid = UUID.randomUUID();
            AttackHarness.send(alice, authFrame("alice", aliceUuid));
            harness.awaitAuthenticated("alice", 5_000);
            // bob has a valid token and claims the UUID of alice
            AttackHarness.send(bob, authFrame("bob", aliceUuid));
            var bobChannel = harness.awaitAuthenticated("bob", 5_000);

            assertFalse(closedWithin(alice, 500), "alice must not be replaced");
            assertEquals(NettyIdentities.derivedUuid("bob"), bobChannel.identity().uuid());
            var aliceChannel = harness.server().clientChannel(NettyIdentities.derivedUuid("alice"));
            assertNotNull(aliceChannel);
            assertEquals("alice", aliceChannel.identity().name());
            assertTrue(aliceChannel.channel().isActive());
            assertNull(harness.server().clientChannel(aliceUuid), "the UUID sent by a client is not used");
            assertEquals(2, harness.server().connections().size());
        } finally {
            harness.stop();
        }
    }

    @Test
    void uuidSquattingFailsWithReject() throws Exception {
        var harness = AttackHarness.start();
        try (var bob = harness.connect(); var alice = harness.connect()) {
            var aliceUuid = UUID.randomUUID();
            // bob comes first and claims the UUID alice will use
            AttackHarness.send(bob, authFrame("bob", aliceUuid));
            harness.awaitAuthenticated("bob", 5_000);
            AttackHarness.send(alice, authFrame("alice", aliceUuid));
            harness.awaitAuthenticated("alice", 5_000);
            assertFalse(closedWithin(alice, 300), "alice must not be blocked");
            assertEquals("alice",
                    harness.server().clientChannel(NettyIdentities.derivedUuid("alice")).identity().name());
        } finally {
            harness.stop();
        }
    }

    @Test
    void clientLearnsItsEffectiveUuid() throws Exception {
        var harness = AttackHarness.start();
        try (var socket = harness.connect()) {
            AttackHarness.send(socket, authFrame("Dave", UUID.randomUUID()));
            harness.awaitAuthenticated("Dave", 5_000);
            // the first frame after authentication is the AuthAcceptedPacket with the effective identity
            socket.setSoTimeout(3_000);
            var in = new java.io.DataInputStream(socket.getInputStream());
            var frame = in.readNBytes(in.readInt());
            var data = new java.io.DataInputStream(new java.io.ByteArrayInputStream(frame));
            var className = new String(data.readNBytes(data.readInt()), java.nio.charset.StandardCharsets.UTF_8);
            assertEquals(de.dataflair.netty.common.packet.auth.AuthAcceptedPacket.class.getName(), className);
            data.readInt(); // content length
            var name = new String(data.readNBytes(data.readInt()), java.nio.charset.StandardCharsets.UTF_8);
            var uuid = new UUID(data.readLong(), data.readLong());
            assertEquals("Dave", name);
            assertEquals(NettyIdentities.derivedUuid("dave"), uuid);
        } finally {
            harness.stop();
        }
    }

    @Test
    void trustedClientUuidIsKeptAndStillConflicts() throws Exception {
        var harness = AttackHarness.start(false, 10_000, server -> server.trustClientUuid(true));
        try (var first = harness.connect(); var second = harness.connect()) {
            var uuid = UUID.randomUUID();
            AttackHarness.send(first, authFrame("erin", uuid));
            assertEquals(uuid, harness.awaitAuthenticated("erin", 5_000).identity().uuid());
            AttackHarness.send(second, authFrame("frank", uuid));
            assertTrue(closedWithin(second, 3_000), "with trusted UUIDs a duplicate UUID is a conflict");
        } finally {
            harness.stop();
        }
    }

    // ---- M2 canonical names ---------------------------------------------------------

    @Test
    void unicodeSpellingsShareOneCanonicalIdentity() throws Exception {
        var harness = AttackHarness.start();
        try (var longS = harness.connect(); var plain = harness.connect();
             var kelvin = harness.connect(); var k = harness.connect()) {
            AttackHarness.send(longS, AttackHarness.authFrame("ſam", AttackHarness.TOKEN));
            harness.awaitAuthenticated("ſam", 5_000);
            AttackHarness.send(plain, AttackHarness.authFrame("sam", AttackHarness.TOKEN));
            assertTrue(closedWithin(plain, 3_000), "sam is the same identity as ſam");

            AttackHarness.send(kelvin, AttackHarness.authFrame("Kevin", AttackHarness.TOKEN));
            harness.awaitAuthenticated("Kevin", 5_000);
            AttackHarness.send(k, AttackHarness.authFrame("kevin", AttackHarness.TOKEN));
            assertTrue(closedWithin(k, 3_000), "kevin is the same identity as the Kelvin sign spelling");

            assertSame(harness.server().firstChannel("SAM"), harness.server().firstChannel("ſam"));
            assertEquals("ſam", harness.server().firstChannel("sam").identity().name());
            assertEquals("Kevin", harness.server().firstChannel("KEVIN").identity().name());
        } finally {
            harness.stop();
        }
    }

    @Test
    void dottedCapitalIIsItsOwnCanonicalName() throws Exception {
        // NFKC + toLowerCase(Locale.ROOT): "İlker" becomes "i̇lker" (with U+0307), not "ilker"; lookup, conflict
        // check and UUID use the same form, so both are separate identities everywhere
        var harness = AttackHarness.start();
        try (var dotted = harness.connect(); var plain = harness.connect()) {
            AttackHarness.send(dotted, AttackHarness.authFrame("İlker", AttackHarness.TOKEN));
            harness.awaitAuthenticated("İlker", 5_000);
            AttackHarness.send(plain, AttackHarness.authFrame("ilker", AttackHarness.TOKEN));
            harness.awaitAuthenticated("ilker", 5_000);
            assertEquals("İlker", harness.server().firstChannel("İLKER").identity().name());
            assertEquals("ilker", harness.server().firstChannel("ILKER").identity().name());
            assertNotEquals(NettyIdentities.derivedUuid("İlker"), NettyIdentities.derivedUuid("ilker"));
        } finally {
            harness.stop();
        }
    }

    // ---- H2 attempts are counted before the filter ----------------------------------------

    @Test
    void parallelFailedAttemptsReachTheFilterAtMostLimitTimes() throws Exception {
        var limit = 5;
        var filterCalls = new AtomicInteger();
        var executor = Executors.newSingleThreadScheduledExecutor();
        var harness = AttackHarness.start(false, 10_000, server -> {
            server.maxAuthFailuresPerMinute(limit);
            server.maxNewConnectionsPerPrefix(0);
            server.addFilter(new AsyncConnectionFilter() {
                @Override
                public @NotNull CompletionStage<Boolean> evaluateAsync(NettyClientChannel.@NotNull AuthType auth) {
                    filterCalls.incrementAndGet();
                    var result = new CompletableFuture<Boolean>();
                    executor.schedule(() -> result.complete(false), 300, TimeUnit.MILLISECONDS);
                    return result;
                }
            });
        });
        var sockets = new ArrayList<Socket>();
        try {
            for (int i = 0; i < 64; i++) {
                sockets.add(harness.connect());
            }
            Thread.sleep(200);
            var start = new CountDownLatch(1);
            var threads = new ArrayList<Thread>();
            for (int i = 0; i < sockets.size(); i++) {
                var socket = sockets.get(i);
                var frame = AttackHarness.authFrame("racer" + i, AttackHarness.TOKEN);
                threads.add(Thread.ofPlatform().start(() -> {
                    try {
                        start.await();
                        AttackHarness.send(socket, frame);
                    } catch (Exception ignored) {
                        // closed by the server
                    }
                }));
            }
            start.countDown();
            for (var thread : threads) {
                thread.join(5_000);
            }
            Thread.sleep(1_500);
            assertTrue(filterCalls.get() >= 1, "at least one attempt reaches the filter");
            assertTrue(filterCalls.get() <= limit, filterCalls.get() + " attempts reached the filter, limit " + limit);
            assertEquals(0, harness.server().connections().size());
        } finally {
            for (var socket : sockets) {
                socket.close();
            }
            harness.stop();
            executor.shutdownNow();
        }
    }

    @Test
    void successfulAttemptsGiveTheirTokenBack() throws Exception {
        var harness = AttackHarness.start(false, 10_000, server -> {
            server.maxAuthFailuresPerMinute(3);
            server.maxNewConnectionsPerPrefix(0);
        });
        try {
            for (int i = 0; i < 10; i++) {
                try (var socket = harness.connect()) {
                    AttackHarness.send(socket, AttackHarness.authFrame("ok" + i, AttackHarness.TOKEN));
                    harness.awaitAuthenticated("ok" + i, 5_000);
                }
            }
        } finally {
            harness.stop();
        }
    }

    // ---- M5 pre-auth slots and connection rate ---------------------------------------

    @Test
    void unauthenticatedConnectionsAreLimited() throws Exception {
        var harness = AttackHarness.start(false, 10_000, server -> {
            server.maxUnauthenticatedConnections(3);
            server.maxNewConnectionsPerPrefix(0);
        });
        var sockets = new ArrayList<Socket>();
        try {
            for (int i = 0; i < 3; i++) {
                sockets.add(harness.connect());
            }
            Thread.sleep(200);
            var fourth = harness.connect();
            sockets.add(fourth);
            assertTrue(closedWithin(fourth, 2_000), "the fourth unauthenticated connection must be closed");
            assertEquals(3, harness.server().connectionLimiter().unauthenticatedConnections());

            // an authenticated connection frees its pre-auth slot
            AttackHarness.send(sockets.getFirst(), AttackHarness.authFrame("slot", AttackHarness.TOKEN));
            harness.awaitAuthenticated("slot", 5_000);
            Thread.sleep(100);
            assertEquals(2, harness.server().connectionLimiter().unauthenticatedConnections());
            var fifth = harness.connect();
            sockets.add(fifth);
            assertFalse(closedWithin(fifth, 500), "a freed slot can be used again");
        } finally {
            for (var socket : sockets) {
                socket.close();
            }
            harness.stop();
        }
    }

    @Test
    void newConnectionsPerPrefixAreRateLimited() throws Exception {
        var harness = AttackHarness.start(false, 10_000, server -> {
            server.maxNewConnectionsPerPrefix(5);
            server.newConnectionWindowMillis(60_000);
        });
        var sockets = new ArrayList<Socket>();
        try {
            for (int i = 0; i < 5; i++) {
                sockets.add(harness.connect());
            }
            Thread.sleep(200);
            for (var socket : sockets) {
                assertFalse(closedWithin(socket, 50), "the first five connections stay");
            }
            var sixth = harness.connect();
            sockets.add(sixth);
            assertTrue(closedWithin(sixth, 2_000), "the sixth new connection within the window must be closed");
        } finally {
            for (var socket : sockets) {
                socket.close();
            }
            harness.stop();
        }
    }

    // ---- L3 duplicate auth packets ------------------------------------------------------

    @Test
    void duplicateAuthPacketsCountAsRejected() throws Exception {
        var harness = AttackHarness.start(false, 10_000, server -> server.maxRejectedPackets(3));
        try (var socket = harness.connect()) {
            AttackHarness.send(socket, AttackHarness.authFrame("dup", AttackHarness.TOKEN));
            harness.awaitAuthenticated("dup", 5_000);
            for (int i = 0; i < 2; i++) {
                AttackHarness.send(socket, AttackHarness.authFrame("dup", AttackHarness.TOKEN));
            }
            assertFalse(closedWithin(socket, 500), "two duplicates are below the limit");
            AttackHarness.send(socket, AttackHarness.authFrame("dup", AttackHarness.TOKEN));
            assertTrue(closedWithin(socket, 3_000), "the third duplicate reaches maxRejectedPackets");
        } finally {
            harness.stop();
        }
    }

    // ---- L1 TLS handshake failures ---------------------------------------------------------

    @Test
    void failedTlsHandshakesBlockThePrefix(@TempDir Path directory) throws Exception {
        var keyStoreFile = directory.resolve("server.p12").toFile();
        SecurityHardeningTest.generateKeyStore(keyStoreFile);
        var keyStore = KeyStore.getInstance("PKCS12");
        try (var in = new FileInputStream(keyStoreFile)) {
            keyStore.load(in, "changeit".toCharArray());
        }
        var keyManagers = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keyManagers.init(keyStore, "changeit".toCharArray());
        var sslContext = SslContextBuilder.forServer(keyManagers).build();

        var harness = AttackHarness.start(false, 10_000, server -> {
            server.sslContext(sslContext);
            server.maxAuthFailuresPerMinute(3);
            server.maxNewConnectionsPerPrefix(0);
        });
        try {
            // a fresh connection waits for its handshake
            try (var quiet = harness.connect()) {
                assertFalse(closedWithin(quiet, 500), "a silent TLS connection is not closed at once");
            }
            for (int i = 0; i < 3; i++) {
                try (var plain = harness.connect()) {
                    AttackHarness.send(plain, AttackHarness.authFrame("plain" + i, AttackHarness.TOKEN));
                    assertTrue(closedWithin(plain, 3_000), "a plain auth packet fails the handshake");
                }
            }
            Thread.sleep(200);
            try (var blocked = harness.connect()) {
                assertTrue(closedWithin(blocked, 1_500), "the prefix is blocked after failed handshakes");
            }
        } finally {
            harness.stop();
        }
    }

    private static byte[] authFrame(String name, UUID uuid) {
        return AttackHarness.frame(AuthPacket.class.getName(),
                AttackHarness.authContent(name, uuid, Map.of("token", AttackHarness.TOKEN)));
    }
}

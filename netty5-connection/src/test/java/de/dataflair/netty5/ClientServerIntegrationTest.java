package de.dataflair.netty5;

import de.dataflair.netty5.actions.ConnectionAction;
import de.dataflair.netty5.client.Netty5Client;
import de.dataflair.netty5.common.packet.auth.AuthPacket;
import de.dataflair.netty5.common.packet.json.JsonPacket;
import de.dataflair.netty5.filter.ConnectionFilter;
import de.dataflair.netty5.server.Netty5Server;
import de.dataflair.netty5.testpackets.TestPacket;
import de.dataflair.netty5.testpackets.TestRequestPacket;
import de.dataflair.netty5.testpackets.TestRespondPacket;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;

class ClientServerIntegrationTest {
    private final List<Netty5Component> components = new ArrayList<>();

    @AfterEach
    void tearDown() throws InterruptedException {
        for (var component : components) {
            component.shutdownGracefully(0, 2, TimeUnit.SECONDS).await(5, TimeUnit.SECONDS);
            if (component instanceof Netty5Server server) {
                server.workerGroup().terminationFuture().await(5, TimeUnit.SECONDS);
            }
        }
    }

    static void await(BooleanSupplier condition, long timeoutMillis, String message) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (!condition.getAsBoolean()) {
            if (System.currentTimeMillis() > deadline) {
                fail("timeout: " + message);
            }
            Thread.sleep(20);
        }
    }

    Netty5Server startServer(int port, Consumer<Netty5Server> config) throws Exception {
        Exception last = null;
        for (int attempt = 0; attempt < 20; attempt++) {
            var server = new Netty5Server("127.0.0.1", port);
            config.accept(server);
            server.initialize();
            components.add(server);
            try {
                server.connectionFuture().get(5, TimeUnit.SECONDS);
                return server;
            } catch (ExecutionException e) {
                last = e;
                if (!(e.getCause() instanceof BindException) && !(e.getCause() instanceof SocketException)) {
                    throw e;
                }
                server.shutdownGracefully(0, 1, TimeUnit.SECONDS).await(5, TimeUnit.SECONDS);
                Thread.sleep(250);
            }
        }
        throw last;
    }

    Netty5Client client(int port, String name) {
        var client = new Netty5Client("127.0.0.1", port, new Netty5ClientChannel.Identity(name, UUID.randomUUID()),
                Map.of("token", "t"));
        client.reconnectDelayMillis(100);
        components.add(client);
        return client;
    }

    Netty5Client connectedClient(Netty5Server server, String name) throws Exception {
        var client = client(server.localAddress().getPort(), name);
        client.initialize();
        client.connectionFuture().get(5, TimeUnit.SECONDS);
        return client;
    }

    static byte[] rawFrame(String className, byte[] content) {
        var name = className.getBytes(StandardCharsets.UTF_8);
        var out = new java.io.ByteArrayOutputStream();
        try (var data = new DataOutputStream(out)) {
            data.writeInt(Integer.BYTES + name.length + Integer.BYTES + content.length);
            data.writeInt(name.length);
            data.write(name);
            data.writeInt(content.length);
            data.write(content);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        return out.toByteArray();
    }

    /**
     * Reads until the peer closes the connection; returns the time it took in milliseconds.
     */
    static long awaitClosedBy(Socket socket, long timeoutMillis) throws IOException {
        socket.setSoTimeout((int) timeoutMillis);
        long start = System.currentTimeMillis();
        try {
            while (socket.getInputStream().read() != -1) {
                // drain
            }
        } catch (SocketException e) {
            // connection reset counts as closed
        }
        return System.currentTimeMillis() - start;
    }

    @Test
    void authenticatesAndExchangesPackets() throws Exception {
        var authenticated = new CompletableFuture<Netty5ClientChannel>();
        var server = startServer(0, s -> s.addAction(new ConnectionAction(authenticated::complete,
                ConnectionAction.State.CLIENT_AUTHENTICATED)));
        server.packetTransmitter().listen(TestPacket.class, (sender, packet) ->
                sender.sendPacket(new TestPacket("pong:" + packet.message(), packet.number() + 1)));

        var client = connectedClient(server, "client-1");
        var received = new CompletableFuture<TestPacket>();
        client.packetTransmitter().listen(TestPacket.class, (sender, packet) -> received.complete(packet));

        var channel = authenticated.get(5, TimeUnit.SECONDS);
        assertEquals("client-1", channel.identity().name());
        await(() -> server.connections().size() == 1, 5_000, "server connection");

        client.thisChannel().sendPacket(new TestPacket("ping", 1));
        var pong = received.get(5, TimeUnit.SECONDS);
        assertEquals("pong:ping", pong.message());
        assertEquals(2, pong.number());
    }

    @Test
    void queriesInBothDirections() throws Exception {
        var server = startServer(0, s -> {
        });
        server.packetTransmitter().listenQuery(TestRequestPacket.class,
                request -> new TestRespondPacket("server:" + request.tag() + request.items()));
        var client = connectedClient(server, "client-q");
        await(() -> server.connections().size() == 1, 5_000, "server connection");

        TestRespondPacket direct = client.packetTransmitter()
                .queryPacketDirect(new TestRequestPacket("direct", List.of("a")), TestRespondPacket.class);
        assertNotNull(direct);
        assertEquals("server:direct[a]", direct.value());

        var async = client.packetTransmitter()
                .queryPacket(new TestRequestPacket("async", List.of()), TestRespondPacket.class)
                .get(5, TimeUnit.SECONDS);
        assertEquals("server:async[]", ((TestRespondPacket) async).value());

        // server -> client through the client's channel transmitter (fix 18)
        client.packetTransmitter().listenQuery(TestRequestPacket.class,
                request -> new TestRespondPacket("client:" + request.tag()));
        var fromClient = server.connections().getFirst().transmitter()
                .queryPacket(new TestRequestPacket("srv", List.of()), TestRespondPacket.class)
                .get(5, TimeUnit.SECONDS);
        assertEquals("client:srv", ((TestRespondPacket) fromClient).value());

        // blocking query from the event loop fails fast (fix 9)
        var error = assertThrows(ExecutionException.class, () -> client.bossGroup().next().submit(() ->
                client.packetTransmitter().queryPacketDirect(new TestRequestPacket("loop", List.of()), TestRespondPacket.class)
        ).get(5, TimeUnit.SECONDS));
        assertInstanceOf(IllegalStateException.class, error.getCause());
    }

    /**
     * Fix 14: unauthenticated connections are closed after the auth timeout.
     */
    @Test
    void unauthenticatedConnectionIsClosedAfterTimeout() throws Exception {
        var server = startServer(0, s -> s.authTimeoutMillis(300));
        try (var socket = new Socket("127.0.0.1", server.localAddress().getPort())) {
            long elapsed = awaitClosedBy(socket, 5_000);
            assertTrue(elapsed >= 200 && elapsed < 5_000, "closed after " + elapsed + " ms");
        }
        assertTrue(server.connections().isEmpty());
    }

    /**
     * Fix 4: a packet other than the auth packet before authentication closes the connection.
     */
    @Test
    void packetBeforeAuthenticationClosesConnection() throws Exception {
        var server = startServer(0, s -> s.authTimeoutMillis(30_000));
        try (var socket = new Socket("127.0.0.1", server.localAddress().getPort())) {
            socket.getOutputStream().write(rawFrame(TestPacket.class.getName(), new byte[0]));
            socket.getOutputStream().flush();
            long elapsed = awaitClosedBy(socket, 5_000);
            assertTrue(elapsed < 5_000, "closed after " + elapsed + " ms");
        }
        assertTrue(server.connections().isEmpty());
    }

    /**
     * Fix 14: a connection rejected by a filter is closed and not registered.
     */
    @Test
    void rejectedConnectionIsClosed() throws Exception {
        var server = startServer(0, s -> s.addFilter(new ConnectionFilter() {
            @Override
            public boolean evaluateFilter(Netty5ClientChannel.AuthType object) {
                return false;
            }
        }));
        var client = client(server.localAddress().getPort(), "rejected");
        client.reconnectDelayMillis(60_000);
        client.initialize();
        client.connectionFuture().get(5, TimeUnit.SECONDS);
        await(() -> client.connectionState() == Netty5Component.ConnectionState.DISCONNECTED, 5_000,
                "client disconnected by server");
        assertTrue(server.connections().isEmpty());
    }

    /**
     * Fix 14: a second auth packet does not register the channel twice.
     */
    @Test
    void duplicateAuthenticationIsIgnored() throws Exception {
        var authCount = new AtomicInteger();
        var server = startServer(0, s -> s.addAction(new ConnectionAction(c -> authCount.incrementAndGet(),
                ConnectionAction.State.CLIENT_AUTHENTICATED)));
        server.packetTransmitter().listen(TestPacket.class, (sender, packet) -> sender.sendPacket(packet));
        var client = connectedClient(server, "dup");
        var echo = new CompletableFuture<TestPacket>();
        client.packetTransmitter().listen(TestPacket.class, (c, p) -> echo.complete(p));

        client.thisChannel().sendPacket(new AuthPacket(client.identity(), Map.of()));
        client.thisChannel().sendPacket(new TestPacket("after", 3));
        assertEquals("after", echo.get(5, TimeUnit.SECONDS).message());
        assertEquals(1, server.connections().size());
        assertEquals(1, authCount.get());
    }

    /**
     * Fix 13: concurrent connects and disconnects keep the connection list consistent.
     */
    @Test
    void concurrentConnectionsAreTracked() throws Exception {
        var disconnects = new AtomicInteger();
        var server = startServer(0, s -> s.addAction(new ConnectionAction(c -> disconnects.incrementAndGet(),
                ConnectionAction.State.CLIENT_DISCONNECTED)));
        int count = 12;
        var clients = new ArrayList<Netty5Client>();
        for (int i = 0; i < count; i++) {
            var client = client(server.localAddress().getPort(), "c" + i);
            client.initialize();
            clients.add(client);
        }
        for (var client : clients) {
            client.connectionFuture().get(5, TimeUnit.SECONDS);
        }
        await(() -> server.connections().size() == count, 5_000, "all clients registered");
        assertNotNull(server.firstChannel("c3"));
        assertNotNull(server.clientChannel(clients.get(5).identity().uuid()));

        for (var client : clients) {
            client.shutdownGracefully(0, 1, TimeUnit.SECONDS);
        }
        await(() -> server.connections().isEmpty(), 5_000, "all clients removed");
        assertEquals(count, disconnects.get());
    }

    /**
     * Fix 7: a client started before the server connects once the server is up, without NPE.
     */
    @Test
    void clientConnectsWhenServerStartsLater() throws Exception {
        int port;
        try (var probe = new ServerSocket(0)) {
            port = probe.getLocalPort();
        }
        var client = client(port, "early");
        client.initialize();
        Thread.sleep(300);
        assertFalse(client.connectionFuture().isDone());
        assertEquals(Netty5Component.ConnectionState.DISCONNECTED, client.connectionState());

        var server = startServer(port, s -> {
        });
        client.connectionFuture().get(10, TimeUnit.SECONDS);
        await(() -> server.connections().size() == 1, 5_000, "server connection");
    }

    /**
     * Fix 16: the client reconnects after losing the connection; listeners survive the reconnect.
     */
    @Test
    void reconnectsAfterServerRestart() throws Exception {
        var server = startServer(0, s -> {
        });
        int port = server.localAddress().getPort();
        var client = connectedClient(server, "reconnect");
        var received = new CompletableFuture<JsonPacket>();
        client.packetTransmitter().listen(JsonPacket.class, (c, p) -> received.complete(p));
        await(() -> server.connections().size() == 1, 5_000, "first connection");

        server.shutdownGracefully(0, 1, TimeUnit.SECONDS).await(5, TimeUnit.SECONDS);
        server.workerGroup().terminationFuture().await(5, TimeUnit.SECONDS);
        await(() -> client.connectionState() == Netty5Component.ConnectionState.DISCONNECTED, 5_000,
                "client noticed lost connection");
        assertFalse(client.connectionFuture().isDone());

        var restarted = startServer(port, s -> {
        });
        client.connectionFuture().get(10, TimeUnit.SECONDS);
        await(() -> restarted.connections().size() == 1, 5_000, "reconnected");

        restarted.connections().getFirst().sendPacket(new JsonPacket("{\"after\":\"restart\"}"));
        assertEquals("{\"after\":\"restart\"}", received.get(5, TimeUnit.SECONDS).json());
    }

    /**
     * Fix 17: the reconnect scheduler runs on a daemon thread and stops on shutdown.
     */
    @Test
    void reconnectSchedulerIsDaemonAndStops() throws Exception {
        int port;
        try (var probe = new ServerSocket(0)) {
            port = probe.getLocalPort();
        }
        var client = client(port, "daemon");
        client.initialize();
        await(() -> Thread.getAllStackTraces().keySet().stream()
                .anyMatch(t -> t.getName().startsWith("netty5-client-reconnect-")), 5_000, "reconnect thread started");
        Thread.getAllStackTraces().keySet().stream()
                .filter(t -> t.getName().startsWith("netty5-client-reconnect-"))
                .forEach(t -> assertTrue(t.isDaemon(), t.getName()));

        client.shutdownGracefully(0, 1, TimeUnit.SECONDS).await(5, TimeUnit.SECONDS);
        assertTrue(client.reconnectScheduler().isShutdown());
        assertTrue(client.reconnectScheduler().awaitTermination(5, TimeUnit.SECONDS));
    }
}

package de.dataflair.netty5.attack;

import de.dataflair.netty5.Netty5ClientChannel;
import de.dataflair.netty5.actions.ConnectionAction;
import de.dataflair.netty5.common.codec.PacketRegistry;
import de.dataflair.netty5.common.packet.auth.AuthPacket;
import de.dataflair.netty5.filter.ConnectionFilter;
import de.dataflair.netty5.server.Netty5Server;
import de.dataflair.netty5.testpackets.TestPacket;
import de.dataflair.netty5.testpackets.TestRequestPacket;
import de.dataflair.netty5.testpackets.TestRespondPacket;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Builds a server configured like {@code netty5-demo} (token filter via a {@link ConnectionFilter}, allowlist
 * registry) and provides helpers that craft raw wire frames by hand, so the attack tests can hit the real server
 * with a plain {@link Socket}.
 * <p>
 * Frame layout, read from {@code PacketEncoder}/{@code PacketDecoder}:
 * <pre>
 *   int outerLength   // everything after this field: nameLen(4) + name + contentLen(4) + content
 *   int nameLength
 *   byte[] className  // UTF-8
 *   int contentLength
 *   byte[] content
 * </pre>
 * "Standard-Limits": the server keeps the library defaults (16 MiB max frame, 64 KiB pre-auth frame).
 */
final class AttackHarness {
    static final String TOKEN = "attack-secret";

    private final Netty5Server server;
    private final int port;
    // must be the SAME instance the CLIENT_AUTHENTICATED action writes to
    private final Map<String, CompletableFuture<Netty5ClientChannel>> authenticated;

    private AttackHarness(Netty5Server server, int port,
                          Map<String, CompletableFuture<Netty5ClientChannel>> authenticated) {
        this.server = server;
        this.port = port;
        this.authenticated = authenticated;
    }

    static AttackHarness start() throws Exception {
        return start(false, 2_000);
    }

    /**
     * @param echo              if true the server echoes every received {@link TestPacket} back to its sender
     *                          (used for the slow-reader backpressure attack)
     * @param authTimeoutMillis unauthenticated-connection timeout
     */
    static AttackHarness start(boolean echo, long authTimeoutMillis) throws Exception {
        return start(echo, authTimeoutMillis, _ -> {
        });
    }

    /**
     * @param config applied after the default configuration, before {@code initialize()}
     */
    static AttackHarness start(boolean echo, long authTimeoutMillis,
                               java.util.function.Consumer<Netty5Server> config) throws Exception {
        var authenticated = new ConcurrentHashMap<String, CompletableFuture<Netty5ClientChannel>>();
        var server = new Netty5Server("127.0.0.1", 0);
        server.serverIdentity(new Netty5ClientChannel.Identity("attack-server", UUID.randomUUID()));
        // allowlist mode (the default), as in the demo
        server.packetRegistry(new PacketRegistry()
                .register(TestPacket.class, TestRequestPacket.class, TestRespondPacket.class, ListBombPacket.class)
);
        // Standard-Limits: library defaults (16 MiB / 64 KiB). Not overridden on purpose.
        server.authTimeoutMillis(authTimeoutMillis);

        server.addFilter(new ConnectionFilter() {
            @Override
            public boolean evaluateFilter(Netty5ClientChannel.AuthType auth) {
                return TOKEN.equals(auth.authProperty().get("token"));
            }
        });
        server.addAction(new ConnectionAction(channel ->
                authenticated.computeIfAbsent(channel.identity().name(), n -> new CompletableFuture<>())
                        .complete(channel),
                ConnectionAction.State.CLIENT_AUTHENTICATED));
        if (echo) {
            server.packetTransmitter().listen(TestPacket.class, (sender, packet) -> sender.sendPacket(packet));
        }

        config.accept(server);
        server.initialize();
        server.connectionFuture().get(5, TimeUnit.SECONDS);
        var address = (InetSocketAddress) server.localAddress();
        return new AttackHarness(server, address.getPort(), authenticated);
    }

    Netty5Server server() {
        return server;
    }

    int port() {
        return port;
    }

    CompletableFuture<Netty5ClientChannel> onAuthenticated(String name) {
        return authenticated.computeIfAbsent(name, n -> new CompletableFuture<>());
    }

    Netty5ClientChannel awaitAuthenticated(String name, long timeoutMillis) throws Exception {
        return onAuthenticated(name).get(timeoutMillis, TimeUnit.MILLISECONDS);
    }

    void stop() {
        try {
            server.shutdownGracefully(0, 2, TimeUnit.SECONDS).await(5, TimeUnit.SECONDS);
            server.workerGroup().terminationFuture().await(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    Socket connect() throws IOException {
        var socket = new Socket();
        socket.connect(new InetSocketAddress("127.0.0.1", port), 5_000);
        socket.setTcpNoDelay(true);
        return socket;
    }

    // ---- frame builders -------------------------------------------------

    static byte[] frame(String className, byte[] content) {
        try {
            var name = className.getBytes(StandardCharsets.UTF_8);
            var out = new ByteArrayOutputStream();
            var data = new DataOutputStream(out);
            int outer = Integer.BYTES + name.length + Integer.BYTES + content.length;
            data.writeInt(outer);
            data.writeInt(name.length);
            data.write(name);
            data.writeInt(content.length);
            data.write(content);
            data.flush();
            return out.toByteArray();
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    /** Frame header announcing {@code contentLength} but carrying no payload yet (for slow/partial sends). */
    static byte[] frameHeaderOnly(String className, int contentLength) {
        try {
            var name = className.getBytes(StandardCharsets.UTF_8);
            var out = new ByteArrayOutputStream();
            var data = new DataOutputStream(out);
            int outer = Integer.BYTES + name.length + Integer.BYTES + contentLength;
            data.writeInt(outer);
            data.writeInt(name.length);
            data.write(name);
            data.writeInt(contentLength);
            data.flush();
            return out.toByteArray();
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    static byte[] authContent(String name, String token) {
        return authContent(name, UUID.randomUUID(), Map.of("token", token));
    }

    static byte[] authContent(String name, UUID uuid, Map<String, String> properties) {
        try {
            var out = new ByteArrayOutputStream();
            var data = new DataOutputStream(out);
            writeString(data, name);
            data.writeLong(uuid.getMostSignificantBits());
            data.writeLong(uuid.getLeastSignificantBits());
            data.writeInt(properties.size());
            for (var entry : properties.entrySet()) {
                writeString(data, entry.getKey());
                writeString(data, entry.getValue());
            }
            data.flush();
            return out.toByteArray();
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    static byte[] authFrame(String name, String token) {
        return frame(AuthPacket.class.getName(), authContent(name, token));
    }

    static byte[] testPacketContent(String message, int number) {
        try {
            var out = new ByteArrayOutputStream();
            var data = new DataOutputStream(out);
            writeString(data, message);
            data.writeInt(number);
            data.flush();
            return out.toByteArray();
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    /** Content of a {@link TestRespondPacket}: 16-byte queryId + string value. */
    static byte[] respondContent(UUID queryId, String value) {
        try {
            var out = new ByteArrayOutputStream();
            var data = new DataOutputStream(out);
            data.writeLong(queryId.getMostSignificantBits());
            data.writeLong(queryId.getLeastSignificantBits());
            writeString(data, value);
            data.flush();
            return out.toByteArray();
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    static void writeString(DataOutputStream data, String value) throws IOException {
        var bytes = value.getBytes(StandardCharsets.UTF_8);
        data.writeInt(bytes.length);
        data.write(bytes);
    }

    static void send(Socket socket, byte[] bytes) throws IOException {
        socket.getOutputStream().write(bytes);
        socket.getOutputStream().flush();
    }
}

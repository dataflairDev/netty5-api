package de.dataflair.netty.demo;

import de.dataflair.netty.NettyClientChannel;
import de.dataflair.netty.NettyIdentities;
import de.dataflair.netty.actions.ConnectionAction;
import de.dataflair.netty.common.codec.PacketRegistry;
import de.dataflair.netty.demo.packets.ChatMessagePacket;
import de.dataflair.netty.demo.packets.UserLookupRequest;
import de.dataflair.netty.demo.packets.UserLookupResponse;
import de.dataflair.netty.filter.ConnectionFilter;
import de.dataflair.netty.filter.ConnectionFilters;
import de.dataflair.netty.server.NettyServer;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

/**
 * Server side of the demo: configuration, filter, actions, listeners and responders.
 */
public final class DemoServer {
    public static final String AUTH_TOKEN_PROPERTY = "token";
    /**
     * At most this many rejected names are remembered; a client that is rejected over and over must not grow
     * the list without bound.
     */
    public static final int MAX_REJECTED_CLIENTS = 100;

    private final NettyServer server;
    private final List<String> receivedChat = new CopyOnWriteArrayList<>();
    private final List<String> rejectedClients = new CopyOnWriteArrayList<>();
    private final CompletableFuture<NettyClientChannel> firstAuthenticated = new CompletableFuture<>();
    private final CompletableFuture<NettyClientChannel> firstDisconnected = new CompletableFuture<>();

    /**
     * @param tokensByName the clients allowed to connect: name and its token. The token is bound to the name, so
     *                     a token holder cannot log in under another client's name.
     */
    public DemoServer(@NotNull String host, int port, @NotNull Map<String, String> tokensByName) {
        // port 0 binds a free port, see port()
        this.server = new NettyServer(host, port);
        server.serverIdentity(new NettyClientChannel.Identity("demo-server", UUID.randomUUID()));

        // Everything below must be configured before initialize().
        // Allowlist mode (the default): only registered classes can be instantiated by a peer. Only AuthPacket
        // is registered from the start.
        server.packetRegistry(packetRegistry());
        // The demo stays on loopback without TLS. Across a network set a server SslContext:
        // server.sslContext(SslContextBuilder.forServer(keyManagerFactory).build());
        // Limits: frames after authentication (default 16 MiB) and before authentication (default 64 KiB).
        server.maxFrameLength(1024 * 1024);
        server.preAuthMaxFrameLength(16 * 1024);
        // Connections that do not send an accepted AuthPacket in time are closed (default 10 s).
        server.authTimeoutMillis(5_000);

        // A ConnectionFilter sees the identity and the auth properties of every AuthPacket. Returning false
        // closes the connection before it is added to connections(). Without any ConnectionFilter the server
        // refuses to start (unless allowAnonymous(true) is set).
        // The token is looked up by the canonical form of the claimed name (NFKC, lower case) and compared in
        // constant time, so a token only works for the name it was issued for, in every spelling. The server
        // derives the UUID of a client from that canonical name; a second connection with the same canonical
        // name is rejected by default (IdentityPolicy.REJECT).
        var allowed = new java.util.HashMap<String, String>();
        tokensByName.forEach((name, token) -> allowed.put(NettyIdentities.canonicalName(name), token));
        server.addFilter(new ConnectionFilter() {
            @Override
            public boolean evaluateFilter(NettyClientChannel.@NotNull AuthType auth) {
                var name = auth.clientChannel().identity().name();
                var expected = allowed.get(auth.canonicalName());
                var accepted = expected != null
                        && ConnectionFilters.tokenEquals(expected, auth.authProperty().get(AUTH_TOKEN_PROPERTY));
                if (!accepted && rejectedClients.size() < MAX_REJECTED_CLIENTS) {
                    rejectedClients.add(name);
                }
                return accepted;
            }
        });

        // Connection actions run on the event loop of the channel; do not block in them.
        server.addAction(new ConnectionAction(channel -> {
            System.out.println("[server] authenticated: " + channel.identity().name());
            // the server sends to one client through its channel
            channel.sendPacket(new ChatMessagePacket("demo-server", "welcome " + channel.identity().name()));
            firstAuthenticated.complete(channel);
        }, ConnectionAction.State.CLIENT_AUTHENTICATED));
        server.addAction(new ConnectionAction(channel -> {
            System.out.println("[server] disconnected: " + channel.identity().name());
            firstDisconnected.complete(channel);
        }, ConnectionAction.State.CLIENT_DISCONNECTED));

        // Listeners and responders on server.packetTransmitter() receive packets of all authenticated clients.
        server.packetTransmitter().listen(ChatMessagePacket.class, (sender, packet) -> {
            System.out.println("[server] chat from " + sender.identity().name() + ": " + packet.text());
            receivedChat.add(packet.text());
        });
        server.packetTransmitter().listenQuery(UserLookupRequest.class, "user-lookup", request ->
                switch (request.userName()) {
                    case "alice" -> new UserLookupResponse(true, "Alice", List.of("admin", "support"));
                    case "bob" -> new UserLookupResponse(true, "Bob", List.of("customer"));
                    default -> UserLookupResponse.notFound();
                });
    }

    /**
     * The registry used on both sides. Each component needs its own instance.
     */
    public static PacketRegistry packetRegistry() {
        return new PacketRegistry()
                .register(ChatMessagePacket.class, UserLookupRequest.class, UserLookupResponse.class);
    }

    /**
     * Binds the server and waits until it is bound.
     */
    public void start() throws Exception {
        server.initialize();
        // completes when the bind has succeeded, exceptionally if it failed
        server.connectionFuture().get(5, TimeUnit.SECONDS);
        System.out.println("[server] listening on " + server.localAddress());
    }

    public int port() {
        var address = server.localAddress();
        if (address == null) {
            throw new IllegalStateException("server is not bound");
        }
        return address.getPort();
    }

    /**
     * Shuts the server down and waits for both event loop groups.
     */
    public void stop() throws InterruptedException {
        // the no-argument variant uses a quiet period of 2 s and a timeout of 15 s
        server.shutdownGracefully(0, 5, TimeUnit.SECONDS).await(10, TimeUnit.SECONDS);
        // the returned future covers the boss group only
        server.workerGroup().terminationFuture().await(10, TimeUnit.SECONDS);
    }

    public NettyServer server() {
        return server;
    }

    public List<String> receivedChat() {
        return receivedChat;
    }

    public List<String> rejectedClients() {
        return rejectedClients;
    }

    public CompletableFuture<NettyClientChannel> firstAuthenticated() {
        return firstAuthenticated;
    }

    public CompletableFuture<NettyClientChannel> firstDisconnected() {
        return firstDisconnected;
    }

    static Map<String, String> authProperties(@NotNull String token) {
        return Map.of(AUTH_TOKEN_PROPERTY, token);
    }
}

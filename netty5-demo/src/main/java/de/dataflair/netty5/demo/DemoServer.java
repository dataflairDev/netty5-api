package de.dataflair.netty5.demo;

import de.dataflair.netty5.Netty5ClientChannel;
import de.dataflair.netty5.actions.ConnectionAction;
import de.dataflair.netty5.common.codec.PacketRegistry;
import de.dataflair.netty5.demo.packets.ChatMessagePacket;
import de.dataflair.netty5.demo.packets.UserLookupRequest;
import de.dataflair.netty5.demo.packets.UserLookupResponse;
import de.dataflair.netty5.filter.ConnectionFilter;
import de.dataflair.netty5.server.Netty5Server;
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

    private final Netty5Server server;
    private final List<String> receivedChat = new CopyOnWriteArrayList<>();
    private final List<String> rejectedClients = new CopyOnWriteArrayList<>();
    private final CompletableFuture<Netty5ClientChannel> firstAuthenticated = new CompletableFuture<>();
    private final CompletableFuture<Netty5ClientChannel> firstDisconnected = new CompletableFuture<>();

    public DemoServer(@NotNull String host, int port, @NotNull String expectedToken) {
        // port 0 binds a free port, see port()
        this.server = new Netty5Server(host, port);
        server.serverIdentity(new Netty5ClientChannel.Identity("demo-server", UUID.randomUUID()));

        // Everything below must be configured before initialize().
        // Allowlist mode: only registered classes can be instantiated by a peer. AuthPacket and the JSON packets
        // are registered by default. Recommended for every server reachable by untrusted peers.
        server.packetRegistry(packetRegistry());
        // Limits: frames after authentication (default 16 MiB) and before authentication (default 64 KiB).
        server.maxFrameLength(1024 * 1024);
        server.preAuthMaxFrameLength(16 * 1024);
        // Connections that do not send an accepted AuthPacket in time are closed (default 10 s).
        server.authTimeoutMillis(5_000);

        // A ConnectionFilter sees the identity and the auth properties of every AuthPacket. Returning false
        // closes the connection before it is added to connections().
        server.addFilter(new ConnectionFilter() {
            @Override
            public boolean evaluateFilter(Netty5ClientChannel.AuthType auth) {
                var accepted = expectedToken.equals(auth.authProperty().get(AUTH_TOKEN_PROPERTY));
                if (!accepted) {
                    rejectedClients.add(auth.clientChannel().identity().name());
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
                .register(ChatMessagePacket.class, UserLookupRequest.class, UserLookupResponse.class)
                .restrictToRegistered(true);
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

    public Netty5Server server() {
        return server;
    }

    public List<String> receivedChat() {
        return receivedChat;
    }

    public List<String> rejectedClients() {
        return rejectedClients;
    }

    public CompletableFuture<Netty5ClientChannel> firstAuthenticated() {
        return firstAuthenticated;
    }

    public CompletableFuture<Netty5ClientChannel> firstDisconnected() {
        return firstDisconnected;
    }

    static Map<String, String> authProperties(@NotNull String token) {
        return Map.of(AUTH_TOKEN_PROPERTY, token);
    }
}

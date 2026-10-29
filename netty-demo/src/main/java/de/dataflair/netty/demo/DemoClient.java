package de.dataflair.netty.demo;

import de.dataflair.netty.NettyClientChannel;
import de.dataflair.netty.client.NettyClient;
import de.dataflair.netty.demo.packets.ChatMessagePacket;
import de.dataflair.netty.demo.packets.UserLookupRequest;
import de.dataflair.netty.demo.packets.UserLookupResponse;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Client side of the demo.
 * <p>
 * The client authenticates automatically: on every (re)connect it sends an AuthPacket with its identity and
 * auth properties. There is no acknowledgement; a rejected client is disconnected by the server and keeps
 * reconnecting until it is shut down.
 */
public final class DemoClient {
    private final NettyClient client;
    private final CompletableFuture<ChatMessagePacket> welcome = new CompletableFuture<>();

    public DemoClient(@NotNull String host, int port, @NotNull String name, @NotNull String token) {
        this.client = new NettyClient(host, port,
                new NettyClientChannel.Identity(name, UUID.randomUUID()),
                DemoServer.authProperties(token));

        // configure before initialize()
        client.packetRegistry(DemoServer.packetRegistry());
        client.maxFrameLength(1024 * 1024);
        client.reconnectDelayMillis(1_000);
        // requests without a response fail after this time (default 5 s)
        client.packetTransmitter().requestTimeoutMillis(3_000);

        // The transmitter lives as long as the client, so listeners and responders registered here survive
        // reconnects. Register them before initialize() to not miss the first packets.
        client.packetTransmitter().listen(ChatMessagePacket.class, (_, packet) -> {
            System.out.println("[" + name + "] chat from " + packet.sender() + ": " + packet.text());
            welcome.complete(packet);
        });
        // the client can answer requests of the server as well
        client.packetTransmitter().listenQuery(UserLookupRequest.class, request ->
                request.userName().equals(name)
                        ? new UserLookupResponse(true, name, List.of("client"))
                        : UserLookupResponse.notFound());
    }

    /**
     * Starts connecting. The returned future completes once the TCP connection is established and the
     * AuthPacket has been written, not when the server has accepted it.
     */
    public CompletableFuture<Void> connect() throws Exception {
        client.initialize();
        return client.connectionFuture();
    }

    /**
     * Packet without a response. Fails with an IllegalStateException while the client is not connected.
     */
    public void sendChat(@NotNull String text) {
        client.packetTransmitter().publishPacket(new ChatMessagePacket(client.identity().name(), text));
    }

    /**
     * Asynchronous request. Safe on any thread, including event loop threads. The future completes
     * exceptionally with a TimeoutException after the request timeout or a ClassCastException on an
     * unexpected response type.
     */
    public CompletableFuture<UserLookupResponse> lookupAsync(@NotNull String userName) {
        return client.packetTransmitter()
                .queryPacket(new UserLookupRequest(userName), UserLookupResponse.class)
                .thenApply(UserLookupResponse.class::cast);
    }

    /**
     * Blocking request. Returns null on timeout. Must not be called on an event loop thread (for example inside
     * a listener, responder or ConnectionAction): it throws an IllegalStateException there, because the response
     * would be processed on the blocked thread. Use {@link #lookupAsync(String)} in those places.
     */
    public UserLookupResponse lookupBlocking(@NotNull String userName) {
        return client.packetTransmitter().queryPacketDirect(new UserLookupRequest(userName), UserLookupResponse.class);
    }

    /**
     * Stops reconnecting, closes the connection and waits for the event loop group.
     */
    public void stop() throws InterruptedException {
        client.shutdownGracefully(0, 5, TimeUnit.SECONDS).await(10, TimeUnit.SECONDS);
    }

    public NettyClient client() {
        return client;
    }

    public CompletableFuture<ChatMessagePacket> welcome() {
        return welcome;
    }
}

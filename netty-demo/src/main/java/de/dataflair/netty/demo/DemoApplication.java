package de.dataflair.netty.demo;

import de.dataflair.netty.demo.packets.UserLookupRequest;
import de.dataflair.netty.demo.packets.UserLookupResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Starts a server and two clients in one JVM, runs every feature once and shuts everything down.
 * <ol>
 *     <li>a client with a valid token authenticates, one with a wrong token is rejected by the filter</li>
 *     <li>the server greets the client from a ConnectionAction, the client publishes a chat message</li>
 *     <li>the client queries the server asynchronously and blocking, the server queries the client</li>
 *     <li>client and server shut down; the server sees the disconnect</li>
 * </ol>
 */
public final class DemoApplication {
    private static final String HOST = "127.0.0.1";
    /**
     * Environment variable with the token of the demo client.
     */
    public static final String TOKEN_VARIABLE = "NETTY_DEMO_TOKEN";
    private static final String FALLBACK_TOKEN = "demo-secret";
    private static final Logger LOGGER = LoggerFactory.getLogger(DemoApplication.class);

    private DemoApplication() {
    }

    public static void main(String[] args) throws Exception {
        var result = run();
        System.out.println("[demo] finished: " + result);
    }

    /**
     * Runs the demo. Throws if any step fails or times out.
     */
    public static Result run() throws Exception {
        var token = token();
        var server = new DemoServer(HOST, 0, Map.of("demo-client", token));
        DemoClient client = null;
        DemoClient intruder = null;
        try {
            server.start();

            // 1. authentication: accepted and rejected client
            client = new DemoClient(HOST, server.port(), "demo-client", token);
            client.connect().get(5, TimeUnit.SECONDS);
            var authenticated = server.firstAuthenticated().get(5, TimeUnit.SECONDS);

            intruder = new DemoClient(HOST, server.port(), "intruder", "wrong-token");
            intruder.connect().get(5, TimeUnit.SECONDS);
            awaitTrue(() -> server.rejectedClients().contains("intruder"), "intruder rejected");
            // a rejected client reconnects forever, so stop it explicitly
            intruder.stop();
            intruder = null;
            System.out.println("[demo] rejected: " + server.rejectedClients());

            // 2. packets without a response
            var welcome = client.welcome().get(5, TimeUnit.SECONDS);
            client.sendChat("hello from the client");
            awaitTrue(() -> !server.receivedChat().isEmpty(), "chat message on the server");

            // 3. requests: asynchronous, blocking (this is the main thread, not an event loop) and server to client
            var alice = client.lookupAsync("alice").get(5, TimeUnit.SECONDS);
            System.out.println("[demo] async lookup alice: " + alice);
            var bob = client.lookupBlocking("bob");
            System.out.println("[demo] blocking lookup bob: " + bob);
            var unknown = client.lookupBlocking("mallory");
            System.out.println("[demo] blocking lookup mallory: " + unknown);

            // The server's own transmitter has no single peer; query a client through its channel transmitter.
            var fromClient = (UserLookupResponse) authenticated.transmitter()
                    .queryPacket(new UserLookupRequest("demo-client"), UserLookupResponse.class)
                    .get(5, TimeUnit.SECONDS);
            System.out.println("[demo] server asked the client: " + fromClient);

            // 4. shutdown: client first, the server sees the disconnect
            client.stop();
            client = null;
            var disconnected = server.firstDisconnected().get(5, TimeUnit.SECONDS);

            return new Result(welcome.text(), List.copyOf(server.receivedChat()), alice, bob, unknown, fromClient,
                    List.copyOf(server.rejectedClients()), disconnected.identity().name());
        } finally {
            if (intruder != null) {
                intruder.stop();
            }
            if (client != null) {
                client.stop();
            }
            server.stop();
            System.out.println("[demo] shut down");
        }
    }

    /**
     * @return the token from {@value #TOKEN_VARIABLE}, or a fixed demo token (with a warning) if it is not set
     */
    static String token() {
        var token = System.getenv(TOKEN_VARIABLE);
        if (token == null || token.isBlank()) {
            LOGGER.warn("{} is not set, using the built-in demo token. Never use a fixed token outside this demo.",
                    TOKEN_VARIABLE);
            return FALLBACK_TOKEN;
        }
        return token;
    }

    private static void awaitTrue(java.util.function.BooleanSupplier condition, String what) throws InterruptedException {
        var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new IllegalStateException("timeout waiting for: " + what);
            }
            Thread.sleep(20);
        }
    }

    public record Result(String welcome,
                         List<String> chatOnServer,
                         UserLookupResponse asyncLookup,
                         UserLookupResponse blockingLookup,
                         UserLookupResponse unknownLookup,
                         UserLookupResponse serverToClientLookup,
                         List<String> rejectedClients,
                         String disconnectedClient) {
    }
}

# netty5-api

A small client/server packet library on top of Netty 4.2. You define packets as Java classes, the library frames,
encodes and decodes them and routes them to listeners. On top of that it offers:

- authentication of every client with an `AuthPacket` (identity plus string properties), checked by filters on
  the server,
- packets without a response (`publishPacket` / `sendPacket`) and `listen` callbacks,
- request/response with `queryPacket` (asynchronous, `CompletableFuture`) and `queryPacketDirect` (blocking),
  answered by responders registered with `listenQuery`,
- automatic reconnect of the client,
- secure defaults: the server refuses to start without a connection filter, only registered packet classes
  are accepted, frames, connections, failed logins and outbound buffers are limited, and TLS is optional.

The `netty5-demo` module contains a complete, runnable example
(`de.dataflair.netty5.demo.DemoApplication`); the snippets below are taken from it.

## Requirements

- Java 27
- an SLF4J 2 binding if you want to see the log output of the library

## Quick start

### Define packets

Every packet class must be public, concrete and have a public constructor taking a `CodecBuffer`. Received
packets are created by `PacketFactory` in two phases: first the `(CodecBuffer)` constructor, then
`readContent(CodecBuffer)`.

A plain packet writes its fields in the sending constructor and reads them in the same order in the
`(CodecBuffer)` constructor:

```java
public class ChatMessagePacket extends Packet {
    private final String sender;
    private final String text;

    public ChatMessagePacket(String sender, String text) {
        this.sender = sender;
        this.text = text;
        buffer.writeString(sender).writeString(text);
    }

    public ChatMessagePacket(CodecBuffer buffer) {
        super(buffer);
        this.sender = buffer.readString();
        this.text = buffer.readString();
    }
    // accessors ...
}
```

Request and response packets extend `RequestPacket` / `RespondPacket`. Their `(CodecBuffer)` constructor only
calls `super(buffer)`; the payload is written in `writeBuffer` and read in `readBuffer`, which the library calls
from `readContent`:

```java
public class UserLookupRequest extends RequestPacket {
    private String userName;

    public UserLookupRequest(String userName) { this.userName = userName; }
    public UserLookupRequest(CodecBuffer buffer) { super(buffer); }

    @Override public void writeBuffer(CodecBuffer b) { b.writeString(userName); }
    @Override public void readBuffer(CodecBuffer b) { this.userName = b.readString(); }
    public String userName() { return userName; }
}

public class UserLookupResponse extends RespondPacket {
    private boolean found;
    private String displayName = "";
    private List<String> roles = new ArrayList<>(); // safe: readBuffer runs after field initialisers

    public UserLookupResponse(boolean found, String displayName, List<String> roles) { /* assign */ }
    public UserLookupResponse(CodecBuffer buffer) { super(buffer); }

    @Override public void writeBuffer(CodecBuffer b) {
        b.writeBoolean(found).writeString(displayName).writeStringList(roles);
    }
    @Override public void readBuffer(CodecBuffer b) {
        found = b.readBoolean();
        displayName = b.readString();
        roles = b.readStringList(new ArrayList<>());
    }
}
```

Pitfall: field initialisers of a subclass run *after* the superclass constructor. Values read in a base class
constructor are overwritten by them. Read into non-final, initialised fields in `readContent` (or `readBuffer`
for request/response packets), not in a base class constructor. To decode a packet yourself, use
`PacketFactory.create(MyPacket.class, buffer)` instead of `new MyPacket(buffer)`.

### Server

```java
var server = new Netty5Server("127.0.0.1", 9000);

// configure everything before initialize()
// allowlist mode is the default: only registered classes (and AuthPacket) can be received
server.packetRegistry(new PacketRegistry()
        .register(ChatMessagePacket.class, UserLookupRequest.class, UserLookupResponse.class));
server.maxFrameLength(1024 * 1024);      // default 16 MiB
server.preAuthMaxFrameLength(16 * 1024); // default 64 KiB
server.authTimeoutMillis(5_000);         // default 10 s

// the token is bound to the canonical form of the claimed name and compared in constant time
var tokensByName = Map.of(Netty5Identities.canonicalName("demo-client"), System.getenv("NETTY5_DEMO_TOKEN"));
server.addFilter(new ConnectionFilter() {
    @Override
    public boolean evaluateFilter(Netty5ClientChannel.AuthType auth) {
        var expected = tokensByName.get(auth.canonicalName());
        return expected != null
                && ConnectionFilters.tokenEquals(expected, auth.authProperty().get("token")); // false closes
    }
});

server.addAction(new ConnectionAction(channel ->
        channel.sendPacket(new ChatMessagePacket("server", "welcome " + channel.identity().name())),
        ConnectionAction.State.CLIENT_AUTHENTICATED));
server.addAction(new ConnectionAction(channel ->
        System.out.println("gone: " + channel.identity().name()),
        ConnectionAction.State.CLIENT_DISCONNECTED));

server.packetTransmitter().listen(ChatMessagePacket.class, (sender, packet) ->
        System.out.println(sender.identity().name() + ": " + packet.text()));
server.packetTransmitter().listenQuery(UserLookupRequest.class, request ->
        new UserLookupResponse(true, "Alice", List.of("admin")));

server.initialize();                                 // throws without a ConnectionFilter
server.connectionFuture().get(5, TimeUnit.SECONDS); // bound (or failed)
```

`initialize()` throws an `IllegalStateException` if neither a `ConnectionFilter` nor an `AsyncConnectionFilter`
is registered. A server that really should accept everybody has to say so with `server.allowAnonymous(true)`.

`server.connections()` holds the authenticated clients; `firstChannel(name)` (canonical name) and
`clientChannel(uuid)` (effective UUID, see [Identity](#identity)) look one up. The server sends to a client with `channel.sendPacket(...)` and queries it through the channel's transmitter,
`channel.transmitter().queryPacket(...)`. `server.packetTransmitter()` itself has no single peer, its query
methods throw `UnsupportedOperationException`.

Filters, listeners, responders and connection actions run on the event loop of the connection and must not
block. A filter that has to wait for a database or another service is an `AsyncConnectionFilter`: it returns a
`CompletionStage<Boolean>`, and the server stops reading from the connection until the stage has completed.

### Client

```java
var client = new Netty5Client("127.0.0.1", 9000,
        new Netty5ClientChannel.Identity("demo-client", UUID.randomUUID()),
        Map.of("token", System.getenv("NETTY5_DEMO_TOKEN")));
client.packetRegistry(new PacketRegistry()
        .register(ChatMessagePacket.class, UserLookupRequest.class, UserLookupResponse.class));

// listeners and responders survive reconnects; register them before initialize()
client.packetTransmitter().listen(ChatMessagePacket.class, (_, packet) -> System.out.println(packet.text()));

client.initialize();
client.connectionFuture().get(5, TimeUnit.SECONDS);
```

`connectionFuture()` completes when the TCP connection (and, with TLS, the handshake) is up and the
`AuthPacket` has been written. `authenticationFuture()` completes when the server has accepted the client; it
yields the identity the server uses (`effectiveIdentity()`). A rejected client is disconnected and keeps
reconnecting (`reconnectDelayMillis`, default 5 s) until it is shut down.

After authentication the client sends a `KeepAlivePacket` whenever it has written nothing for
`keepAliveIntervalMillis` (default 60 s, `0` disables it), so the server's idle timeout does not close a quiet
connection. Keep it below the server's `idleTimeoutMillis`. A client that sends a lot should check
`client.isWritable()` (or `channel.isWritable()`) and wait while it is false; above `maxOutboundBufferBytes`
pending bytes the connection is closed.

### Identity

- The server does not trust the UUID a client sends. It derives the UUID from the **canonical name**:
  `Netty5Identities.derivedUuid(name)` = `UUID.nameUUIDFromBytes("netty5-api:identity:" + canonicalName(name))`.
  `server.clientChannel(uuid)` therefore finds a client by this derived UUID; the client gets it from the
  `AuthAcceptedPacket` (`client.effectiveIdentity()`), or computes it with `Netty5Identities.derivedUuid`.
- The canonical name is `Netty5Identities.canonicalName(name)`: Unicode NFKC, then `toLowerCase(Locale.ROOT)`.
  `ſam`, `Sam` and `SAM` are one identity, the Kelvin sign in `\u212Aevin` becomes `kevin`, and the result does
  not depend on the default locale. `İlker` stays distinct from `ilker` (its canonical form keeps the combining
  dot). The conflict check of the `identityPolicy`, `clientChannel(name)` and `firstChannel(name)` all use it.
- **The filter has to check the canonical name against the token** (`auth.canonicalName()`, as in the server
  example), so a token issued for one name works for every spelling of it and for nothing else.
- `server.trustClientUuid(true)` makes the server use the UUID from the `AuthPacket`. Then any client with a valid
  token could claim (`REPLACE_OLD`) or block (`REJECT`) the UUID of another one, so **the filter must also check
  that the client may use that UUID**.

### Publish and query

```java
var transmitter = client.packetTransmitter();

// no response
transmitter.publishPacket(new ChatMessagePacket("demo-client", "hello"));

// asynchronous; fails with TimeoutException after requestTimeoutMillis (default 5 s)
transmitter.queryPacket(new UserLookupRequest("alice"), UserLookupResponse.class)
        .thenApply(UserLookupResponse.class::cast)
        .thenAccept(response -> System.out.println(response.displayName()));

// blocking; returns null on timeout
UserLookupResponse bob = transmitter.queryPacketDirect(new UserLookupRequest("bob"), UserLookupResponse.class);
```

`queryPacketDirect` must not be called on an event loop thread, which includes listeners, responders and
`ConnectionAction`s. It throws an `IllegalStateException` there; use `queryPacket` in those places.

### Shutdown

```java
client.shutdownGracefully(0, 5, TimeUnit.SECONDS).await(10, TimeUnit.SECONDS);

server.shutdownGracefully(0, 5, TimeUnit.SECONDS).await(10, TimeUnit.SECONDS);
server.workerGroup().terminationFuture().await(10, TimeUnit.SECONDS); // the returned future covers the boss group only
```

`shutdownGracefully()` without arguments uses a quiet period of 2 s and a timeout of 15 s. The client stops
reconnecting when it is shut down.

### Run the demo

```
./gradlew :netty5-demo:run
```

It starts a server and two clients (one with a wrong token) in one JVM, runs every step above once and exits.
The token of the demo client is read from the environment variable `NETTY5_DEMO_TOKEN`; without it the demo
falls back to a built-in token and logs a warning.
`./gradlew :netty5-demo:test` runs the same flow as a test.

## Security notes

- **Fail-closed start.** `Netty5Server.initialize()` throws if no `ConnectionFilter` or `AsyncConnectionFilter`
  is registered, unless `allowAnonymous(true)` is set (which is logged as a warning).
- **Allowlist.** Incoming packets carry their class name. By default the `PacketRegistry` only accepts classes
  registered with `register(...)` plus `AuthPacket`; the built-in JSON packets have to be registered like any
  other packet. `allowAnyPacketClass()` switches to the open mode, which lets the peer instantiate every public,
  concrete packet class on the class path; only use it between trusted peers. Any error while a packet class is
  loaded or constructed (including `ExceptionInInitializerError` and `StackOverflowError`) closes the connection.
- **Authentication and identity.** Until an `AuthPacket` has passed all filters, every other packet closes the
  connection, and connections that do not authenticate within `authTimeoutMillis` (default 10 s) are closed. Only
  the first `AuthPacket` of a connection is evaluated, later ones count as rejected packets. The filter decides
  which identity a client may use: bind the token to the canonical name (see [Identity](#identity)) and compare
  it with `ConnectionFilters.tokenEquals`. A second connection with the canonical name or effective UUID of an
  authenticated one is rejected by default; `identityPolicy(IdentityPolicy.REPLACE_OLD)` closes the old
  connection instead.
- **Authorization.** The library does not know which packets a client may send. Check this in a
  `PacketReceiveFilter` (sender and packet). A response that does not answer a pending request is dropped and
  never reaches a listener.
- **Limits.**
  - Frames: `maxFrameLength` (default 16 MiB) after authentication, `preAuthMaxFrameLength` (default 64 KiB)
    before. The pre-auth limit is checked as soon as the length field arrives, so an unauthenticated peer cannot
    make the server buffer more than that. An `AuthPacket` carries at most 256 properties.
  - Connections: `maxConnections` (default 10000), `maxUnauthenticatedConnections` (default 512) and
    `maxConnectionsPerAddress` (default 64 per prefix), checked by the first handler of every connection;
    `backlog` sets `SO_BACKLOG` (default 1024).
  - New connections: `maxNewConnectionsPerPrefix` per `newConnectionWindowMillis` (default 30 per 10 s, token
    bucket; `0` disables it).
  - Authentication attempts: `maxAuthFailuresPerMinute` (default 10) per prefix. Every attempt takes a token when
    it starts and gives it back on success, so parallel guesses are limited too; failed or timed out TLS
    handshakes cost a token as well. A prefix without tokens is closed right away until its budget has refilled.
  - Prefixes: IPv4 per address, IPv6 per `ipv6PrefixLength` bits (default /64). Each limiter tracks at most
    `maxTrackedPrefixes` (default 100000, least recently used dropped first); refilled prefixes are removed every
    10 s.
  - Rejected packets: after `maxRejectedPackets` (default 16, server and client) decode errors, filter
    rejections or duplicate `AuthPacket`s a connection is closed.
  - Idle connections: `idleTimeoutMillis` (default 5 min) closes an authenticated connection from which the
    server received nothing, its own writes do not count; `0` disables it with a warning. The client's
    keep-alive (`keepAliveIntervalMillis`, default 60 s) keeps a quiet but healthy client connected; the server
    drops `KeepAlivePacket`s before any filter or listener. `authTimeoutMillis <= 0` is allowed but logged as a
    warning.
  - Deserialisation: `CodecBuffer.readList`/`readMap` check the announced size against the readable bytes, stop
    if an element consumes no byte and have overloads with a `maxSize`; `readEnum` checks the ordinal;
    `readJsonObject()` reads at most 1 MiB (`readJsonObject(maxLength)` for other limits).
- **Backpressure.** `writeBufferWaterMark` (default 32/64 KiB) controls when a connection becomes unwritable
  (`isWritable()`); packets queued from other threads are counted with their content size. While a server
  connection is unwritable the server stops reading from it; the client keeps reading, so two peers that both
  send a lot cannot block each other. If the pending bytes exceed `maxOutboundBufferBytes` (default 8 MiB) the
  connection is closed.
- **TLS.** Without `sslContext` the connection is plain TCP and the auth properties travel in clear text; the
  server logs a warning when it binds a non-loopback address without TLS. With a server `SslContext` the
  `SslHandler` is the first handler that sees the bytes, and the handshake timeout (`handshakeTimeoutMillis`,
  default 10 s) is capped at the authentication timeout. A client `SslContext` verifies the host name
  (`endpointIdentificationAlgorithm=HTTPS`) and the client sends its `AuthPacket` only after the handshake.
- **Logging.** The library logs through SLF4J. Values chosen by the peer are cut to 200 characters and stripped
  of control characters, and log lines are throttled per connection.

## Migrating from 1.x

See [CHANGELOG.md](CHANGELOG.md) for the breaking changes in 2.0.0 (Netty 4.2, Java 27, packet construction,
query wire format, limits and authentication).

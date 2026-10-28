# netty5-api

A small client/server packet library on top of Netty 4.2. You define packets as Java classes, the library frames,
encodes and decodes them and routes them to listeners. On top of that it offers:

- authentication of every client with an `AuthPacket` (identity plus string properties), checked by filters on
  the server,
- packets without a response (`publishPacket` / `sendPacket`) and `listen` callbacks,
- request/response with `queryPacket` (asynchronous, `CompletableFuture`) and `queryPacketDirect` (blocking),
  answered by responders registered with `listenQuery`,
- automatic reconnect of the client,
- a packet registry with an allowlist mode, frame limits and an authentication timeout.

The `netty5-demo` module contains a complete, runnable example
(`de.dataflair.netty5.demo.DemoApplication`); the snippets below are taken from it.

## Requirements

- Java 27

## Dependency

The library is published to the public Dataflair repository.

Gradle (Kotlin DSL):

```kotlin
repositories {
    mavenCentral()
    maven("https://rp1.dataflair.cloud/repository/dataflair-public-release/")
}

dependencies {
    implementation("de.dataflair.netty5-api:netty5-connection:2.0.0")
}
```

Maven:

```xml
<repositories>
    <repository>
        <id>dataflair-public-release</id>
        <url>https://rp1.dataflair.cloud/repository/dataflair-public-release/</url>
    </repository>
</repositories>

<dependency>
    <groupId>de.dataflair.netty5-api</groupId>
    <artifactId>netty5-connection</artifactId>
    <version>2.0.0</version>
</dependency>
```

Netty and Gson come in as transitive dependencies.

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
server.packetRegistry(new PacketRegistry()
        .register(ChatMessagePacket.class, UserLookupRequest.class, UserLookupResponse.class)
        .restrictToRegistered(true));
server.maxFrameLength(1024 * 1024);      // default 16 MiB
server.preAuthMaxFrameLength(16 * 1024); // default 64 KiB
server.authTimeoutMillis(5_000);         // default 10 s

server.addFilter(new ConnectionFilter() {
    @Override
    public boolean evaluateFilter(Netty5ClientChannel.AuthType auth) {
        return "demo-secret".equals(auth.authProperty().get("token")); // false closes the connection
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

server.initialize();
server.connectionFuture().get(5, TimeUnit.SECONDS); // bound (or failed)
```

`server.connections()` holds the authenticated clients; `firstChannel(name)` and `clientChannel(uuid)` look one
up. The server sends to a client with `channel.sendPacket(...)` and queries it through the channel's transmitter,
`channel.transmitter().queryPacket(...)`. `server.packetTransmitter()` itself has no single peer, its query
methods throw `UnsupportedOperationException`.

### Client

```java
var client = new Netty5Client("127.0.0.1", 9000,
        new Netty5ClientChannel.Identity("demo-client", UUID.randomUUID()),
        Map.of("token", "demo-secret"));
client.packetRegistry(new PacketRegistry()
        .register(ChatMessagePacket.class, UserLookupRequest.class, UserLookupResponse.class)
        .restrictToRegistered(true));

// listeners and responders survive reconnects; register them before initialize()
client.packetTransmitter().listen(ChatMessagePacket.class, (_, packet) -> System.out.println(packet.text()));

client.initialize();
client.connectionFuture().get(5, TimeUnit.SECONDS);
```

`connectionFuture()` completes when the TCP connection is up and the `AuthPacket` has been written. There is no
acknowledgement of the authentication: a rejected client is disconnected and keeps reconnecting
(`reconnectDelayMillis`, default 5 s) until it is shut down.

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
`./gradlew :netty5-demo:test` runs the same flow as a test.

## Security notes

- **Allowlist mode.** Incoming packets carry their class name. In the default open mode the registry accepts
  every public, concrete packet class on the class path. For servers reachable by untrusted peers enable
  `restrictToRegistered(true)` and register your packet classes; `AuthPacket` and the JSON packets are
  registered by default.
- **Frame limits.** Frames are limited to `maxFrameLength` (default 16 MiB); larger incoming frames close the
  connection, larger outgoing packets fail the write. Before authentication the limit is
  `preAuthMaxFrameLength` (default 64 KiB) and an `AuthPacket` may carry at most 256 properties. Set the limits
  to what your packets actually need.
- **Authentication.** Until an `AuthPacket` has passed all `ConnectionFilter`s, every other packet closes the
  connection. Connections that do not authenticate within `authTimeoutMillis` (default 10 s) are closed. The
  auth properties are sent in plain text; the library does not encrypt the connection.

## Migrating from 1.x

See [CHANGELOG.md](CHANGELOG.md) for the breaking changes in 2.0.0 (Netty 4.2, Java 27, packet construction,
query wire format, limits and authentication).

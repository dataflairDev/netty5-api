# Changelog

## 2.0.0

### Platform

- Migrated from Netty 5 Alpha5 to Netty 4.2.19. All imports change from `io.netty5.*` to `io.netty.*`.
- Requires Java 27.

### Packet construction

- Received packets are built by `PacketFactory`: first the public `(CodecBuffer)` constructor, then
  `Packet#readContent(CodecBuffer)`. Request and respond packets read their payload in `readContent`, so field
  initialisers of subclasses no longer overwrite decoded values. Code that created packets with
  `new X(buffer)` to decode them must use `PacketFactory.create(X.class, buffer)`.
- A received packet keeps its content (the buffer is rewound after decoding), so it can be forwarded unchanged
  with `publishPacket`. Forwarding a received request with `queryPacket`, or returning a received response from a
  responder, rewrites query id and content instead of appending them.

### Removed and changed API

- `Netty5PacketTransmitter.NullSimulationPacket` was removed.
- `futureRequests` and `directRequests` were replaced by `pendingRequests`
  (`Map<UUID, CompletableFuture<Packet>>`); requests time out after `requestTimeoutMillis` (default 5 s).
- `Netty5Server.connections(List)` was removed. `connections()` is a thread-safe list that only contains
  authenticated channels.
- `PacketDecoder` now extends `MessageToMessageDecoder<ByteBuf>` instead of `ByteToMessageDecoder`.
- `Netty5Component.deployFuture()` returns a `ChannelFutureListener` instead of `FutureListener<? super Channel>`.

### Limits and authentication

- Frames are limited to 16 MiB (`Netty5Component#maxFrameLength`). Larger incoming frames close the channel;
  larger outgoing packets fail the write promise with a `TooLongFrameException`.
- Before authentication frames are limited to 64 KiB (`Netty5Component#preAuthMaxFrameLength`) and an
  `AuthPacket` may carry at most 256 properties.
- The server requires authentication: until an `AuthPacket` has been accepted, every other packet is rejected
  and closes the connection. Connections that do not authenticate within 10 s are closed
  (`Netty5Server#authTimeoutMillis`).
- Framing errors (for example a negative length field) close the connection. A packet rejected after
  authentication is dropped and the connection stays open.

### Wire format

- `QueryPacket` no longer declares its own query id and no longer reads it a second time; it now uses the wire
  format of `RequestPacket`. **Queries are incompatible with 1.0.22 peers**; plain packets are not affected.

### Packet registry

- Incoming class names are resolved by `PacketRegistry`. **Allowlist mode is the default** on server and client:
  only classes passed to `register(...)` and `AuthPacket` are accepted. The JSON packets (`JsonPacket`,
  `JsonRequestPacket`, `JsonRespondPacket`) are no longer registered automatically. `restrictToRegistered(boolean)`
  was removed; `allowAnyPacketClass()` switches to the open mode, in which every public, concrete packet class on
  the class path can be instantiated by the peer.
- Any `Throwable` thrown while a packet class is resolved or constructed (`ExceptionInInitializerError`,
  `StackOverflowError`, `LinkageError`, ...) drops the frame and closes the connection. `PacketFactory.create`
  rethrows an `Error` from the constructor instead of wrapping it.

### Security

- **Fail-closed server.** `Netty5Server.initialize()` throws an `IllegalStateException` if no `ConnectionFilter`
  or `AsyncConnectionFilter` is registered, unless `allowAnonymous(true)` is set.
- **Pre-auth buffering.** The new `Netty5FrameDecoder` (a `LengthFieldBasedFrameDecoder`) checks the announced
  frame length against `preAuthMaxFrameLength` as soon as the length field has arrived and closes the connection
  with a `TooLongFrameException`; before, up to `maxFrameLength` was buffered per unauthenticated connection.
- **Connection limits.** `ConnectionLimiter`, the first handler of every server connection, limits open
  connections globally (`maxConnections`, default 10000) and per remote address (`maxConnectionsPerAddress`,
  default 64). `SO_BACKLOG` is set (`backlog`, default 1024). `AuthFailureLimiter` throttles failed
  authentications per address (`maxAuthFailuresPerMinute`, default 10); a blocked address is closed right away.
- **Backpressure.** `WRITE_BUFFER_WATER_MARK` is set on server and client (`writeBufferWaterMark`, default
  32/64 KiB). `BackpressureHandler` stops reading while a channel is unwritable and closes it when the outbound
  buffer exceeds `maxOutboundBufferBytes` (default 8 MiB).
- **Identity.** A second connection with the name (case-insensitive) or UUID of an authenticated one is rejected
  (`identityPolicy`, default `IdentityPolicy.REJECT`; `REPLACE_OLD` closes the old connection).
- **TLS.** Optional `sslContext` on server and client (new dependency `io.netty:netty-handler`). The client
  verifies the host name and sends its `AuthPacket` after the handshake; the handshake timeout
  (`handshakeTimeoutMillis`, default 10 s) is capped at the server's authentication timeout. Binding a
  non-loopback address without TLS logs a warning.
- **Async filters.** `AsyncConnectionFilter` returns a `CompletionStage<Boolean>`; reading is suspended until it
  completes, the result is processed on the channel's event loop.
- **Authentication.** Only the first `AuthPacket` of a connection is evaluated; once a connection is closed or
  rejected nothing that was already buffered is processed. An unexpected exception before authentication now
  closes the connection.
- **Rejected packets.** After `maxRejectedPackets` (default 16) rejected packets an authenticated connection is
  closed (the client closes after 16 as well). Packets rejected by a `PacketReceiveFilter` count too.
- **Unsolicited responses.** A `RespondPacket` that does not answer a pending request is dropped and no longer
  passed to listeners.
- **Idle timeout.** Authenticated server connections are closed after `idleTimeoutMillis` (default 5 min) without
  traffic; `0` disables it with a warning. `authTimeoutMillis <= 0` logs a warning.
- **Deserialisation.** `CodecBuffer.readList`/`readMap` validate the announced size against the readable bytes,
  abort when an element consumes no byte and have overloads with `maxSize`. `readString(maxLength)` and
  `readJsonObject(maxLength)` were added; `readJsonObject()` reads at most 1 MiB. `readEnum` validates the
  ordinal. `AuthPacket` uses `readMap(..., MAX_PROPERTIES)`.
- **Helpers.** `ConnectionFilters.tokenEquals` and `constantTimeEquals` compare secrets in constant time.

### Logging

- All output goes through SLF4J (`org.slf4j:slf4j-api`, exposed as an `api` dependency); nothing is written to
  `System.out`/`System.err` any more. Peer controlled values are cut to 200 characters and stripped of control
  characters (`SafeLog`), log lines are throttled per channel.

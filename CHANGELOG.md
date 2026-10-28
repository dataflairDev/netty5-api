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
- `maxRejectedPackets` and `DEFAULT_MAX_REJECTED_PACKETS` moved from `Netty5Server` to `Netty5Component`, so the
  client has the setting as well.
- `AuthFailureLimiter` counts attempts (`tryAcquire`/`release`) instead of only failures; `ConnectionLimiter` has a
  new constructor with the pre-auth slot limit, the connection rate limiter and the IPv6 prefix length.

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

- After a successful authentication the server sends an `AuthAcceptedPacket` with the effective identity, and the
  client sends a `KeepAlivePacket` when it has been quiet for `keepAliveIntervalMillis`. Both are registered in
  every `PacketRegistry` (together with `AuthPacket`), so both peers need 2.0.0.

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
  connections globally (`maxConnections`, default 10000), connections that have not authenticated yet
  (`maxUnauthenticatedConnections`, default 512) and connections per prefix (`maxConnectionsPerAddress`, default
  64), and rate limits new connections per prefix with a token bucket (`maxNewConnectionsPerPrefix` per
  `newConnectionWindowMillis`, default 30 per 10 s, `0` disables it). `SO_BACKLOG` is set (`backlog`, default
  1024).
- **Authentication throttling.** `AuthFailureLimiter` (`maxAuthFailuresPerMinute`, default 10) takes a token
  atomically when an authentication attempt starts and gives it back when the attempt succeeds, so parallel
  attempts cannot all pass the check before the first failure is counted. Failed and timed out TLS handshakes are
  counted as well. A blocked prefix is closed right away.
- **Prefixes.** All per-address limits count IPv4 per address (/32) and IPv6 per /64 (`ipv6PrefixLength`). Each
  rate limiter keeps at most `maxTrackedPrefixes` (default 100000) prefixes and drops the least recently used one
  beyond that; refilled prefixes are removed every 10 s on the server's event loop instead of on every failure.
- **Backpressure.** `WRITE_BUFFER_WATER_MARK` is set on server and client (`writeBufferWaterMark`, default
  32/64 KiB). The server stops reading while a channel is unwritable; the client keeps reading, otherwise two peers
  that both send a lot could wait for each other forever. Both close a channel whose outbound buffer exceeds
  `maxOutboundBufferBytes` (default 8 MiB). `PacketSizeEstimator` (content bytes plus overhead) is installed as
  `MESSAGE_SIZE_ESTIMATOR`, so packets queued from other threads count towards the water marks before they are
  encoded. `Netty5ClientChannel.isWritable()` and `Netty5Client.isWritable()` were added.
- **Identity.** The server derives the UUID of a client from its canonical name
  (`Netty5Identities.derivedUuid`: `UUID.nameUUIDFromBytes` of a fixed prefix plus the canonical name); the UUID
  in the `AuthPacket` is ignored unless `trustClientUuid(true)` is set. The canonical name
  (`Netty5Identities.canonicalName`: NFKC, then `toLowerCase(Locale.ROOT)`) is used for the conflict check and for
  `clientChannel(name)`/`firstChannel(name)`; `AuthType.canonicalName()` gives filters the same form. A second
  connection with the canonical name or effective UUID of an authenticated one is rejected (`identityPolicy`,
  default `IdentityPolicy.REJECT`; `REPLACE_OLD` closes the old connection). The client learns its effective
  identity from the `AuthAcceptedPacket` (`Netty5Client.effectiveIdentity()`, `authenticationFuture()`).
- **TLS.** Optional `sslContext` on server and client (new dependency `io.netty:netty-handler`). The client
  verifies the host name and sends its `AuthPacket` after the handshake; the handshake timeout
  (`handshakeTimeoutMillis`, default 10 s) is capped at the server's authentication timeout. Binding a
  non-loopback address without TLS logs a warning.
- **Async filters.** `AsyncConnectionFilter` returns a `CompletionStage<Boolean>`; reading is suspended until it
  completes, the result is processed on the channel's event loop.
- **Authentication.** Only the first `AuthPacket` of a connection is evaluated; every later one counts as a
  rejected packet. Once a connection is closed or rejected nothing that was already buffered is processed. An
  unexpected exception before authentication now closes the connection.
- **Rejected packets.** After `maxRejectedPackets` (default 16, configurable on server and client) rejected
  packets a connection is closed. Packets rejected by a `PacketReceiveFilter` and duplicate `AuthPacket`s count
  too.
- **Unsolicited responses.** A `RespondPacket` that does not answer a pending request is dropped and no longer
  passed to listeners.
- **Idle timeout and keep-alive.** Authenticated server connections are closed after `idleTimeoutMillis`
  (default 5 min) in which the server received nothing (reader idle; its own writes do not keep a mute peer
  alive); `0` disables it with a warning. The client sends a `KeepAlivePacket` after `keepAliveIntervalMillis`
  (default 60 s, `0` disables it) without writing; the server drops it after authentication before any filter or
  listener and does not count it as rejected. `authTimeoutMillis <= 0` logs a warning.
- **Deserialisation.** `CodecBuffer.readList`/`readMap` validate the announced size against the readable bytes,
  abort when an element consumes no byte and have overloads with `maxSize`. `readString(maxLength)` and
  `readJsonObject(maxLength)` were added; `readJsonObject()` reads at most 1 MiB. `readEnum` validates the
  ordinal. `AuthPacket` uses `readMap(..., MAX_PROPERTIES)`.
- **Helpers.** `ConnectionFilters.tokenEquals` and `constantTimeEquals` compare secrets in constant time.

### Logging

- All output goes through SLF4J (`org.slf4j:slf4j-api`, exposed as an `api` dependency); nothing is written to
  `System.out`/`System.err` any more. Peer controlled values are cut to 200 characters and stripped of control
  characters (`SafeLog`), log lines are throttled per channel. Log lines without a channel (`CodecBuffer`) are
  throttled with `SafeLog.throttle()`.

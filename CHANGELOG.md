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

- Incoming class names are resolved by `PacketRegistry`. In the default open mode every public, concrete packet
  class on the class path can be instantiated by the peer. Servers reachable by untrusted peers should enable the
  allowlist mode with `restrictToRegistered(true)` and register their packet classes with `register(...)`.

package de.dataflair.netty5.server;



/*
 * Copyright 2023-2024 netty5-api contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import de.dataflair.netty5.Netty5ClientChannel;
import de.dataflair.netty5.TriConsumer;
import de.dataflair.netty5.common.packet.*;
import io.netty.channel.EventLoopGroup;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Server-wide transmitter. Listeners and responders registered here apply to all clients.
 * <p>
 * A server has no single peer, so requests cannot be sent through this transmitter: all {@code queryPacket}
 * variants throw {@link UnsupportedOperationException}. To query a client use the transmitter of its channel,
 * e.g. {@code server.clientChannel(uuid).transmitter().queryPacket(...)}; responses from that client are routed
 * back to it.
 */
public final class Netty5ServerPacketTransmitter extends Netty5PacketTransmitter {
    private static final String UNSUPPORTED = "The server transmitter has no single peer; query a client through "
            + "its channel: server.clientChannel(...).transmitter().queryPacket(...)";

    private final TriConsumer<RequestPacket, Class<Packet>, Consumer<Packet>> requestPacketConsumer;

    public Netty5ServerPacketTransmitter(EventLoopGroup eventExecutors,
                                         Consumer<Packet> packetConsumer,
                                         TriConsumer<RequestPacket, Class<Packet>, Consumer<Packet>> requestPacketConsumer) {
        super(eventExecutors, packetConsumer);
        this.requestPacketConsumer = requestPacketConsumer;
    }

    /**
     * @throws UnsupportedOperationException always, see the class documentation
     */
    @Override
    public <P extends Packet> CompletableFuture<Packet> queryPacket(@NotNull RequestPacket requestPacket, @NotNull Class<P> packet) {
        throw new UnsupportedOperationException(UNSUPPORTED);
    }

    /**
     * @throws UnsupportedOperationException always, see the class documentation
     */
    @Override
    public <P extends Packet> P queryPacketDirect(@NotNull RequestPacket requestPacket, @NotNull Class<P> packetClass) {
        throw new UnsupportedOperationException(UNSUPPORTED);
    }

    /**
     * @throws UnsupportedOperationException always, see the class documentation
     */
    @Override
    public <P extends Packet> void queryPacket(@NotNull RequestPacket requestPacket, @NotNull Class<P> packet, @NotNull Consumer<P> callback) {
        throw new UnsupportedOperationException(UNSUPPORTED);
    }

    public <R extends RequestPacket> void callResponder(@NotNull R request, @NotNull Netty5ClientChannel sender) {
        var registered = responders().get(request.getClass());
        if (registered != null) {
            registered.forEach((_, respondPacketFunction) ->
                    sender.transmitter().publishPacket(createResponse(request, respondPacketFunction)));
        } else {
            sender.transmitter().callResponder(request);
        }
    }

    @Override
    public void call(@NotNull Packet packet, @Nullable Netty5ClientChannel sender) {
        if (packet instanceof RespondPacket respondPacket) {
            // responses to requests sent through a client channel's transmitter
            if (sender != null && sender.transmitter() != null) {
                sender.transmitter().completeRequest(respondPacket);
            }
            this.completeRequest(respondPacket);
        }

        if (packet instanceof RequestPacket requestPacket) {
            if (sender != null) {
                this.callResponder(requestPacket, sender);
            } else {
                throw new IllegalArgumentException("Sender cannot be null for a RequestPacket received by the server");
            }
        }

        this.callActions(packet, sender);
        this.callListeners(packet, sender);
    }

    @Override
    public void publishPacket(@NotNull Packet packet) {
        packetConsumer().accept(packet);
    }

    @Override
    public void callActions(@NotNull Packet packet, @Nullable Netty5ClientChannel sender) {
    }
}

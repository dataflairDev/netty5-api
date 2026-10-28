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

import de.dataflair.netty5.Netty5ChannelUtils;
import de.dataflair.netty5.Netty5ClientChannel;
import de.dataflair.netty5.actions.ConnectionAction;
import de.dataflair.netty5.client.Netty5ClientPacketTransmitter;
import de.dataflair.netty5.common.codec.PacketRejectedException;
import de.dataflair.netty5.common.packet.Packet;
import de.dataflair.netty5.common.packet.auth.AuthPacket;
import de.dataflair.netty5.filter.ConnectionFilter;
import de.dataflair.netty5.filter.PacketReceiveFilter;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.TooLongFrameException;
import io.netty.util.concurrent.ScheduledFuture;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Server side handler. One instance per child channel; its state is only touched on the channel's event loop.
 */
public final class Netty5ServerHandler extends SimpleChannelInboundHandler<Packet> {
    private final Netty5Server server;
    private Netty5ClientChannel clientChannel;
    private ScheduledFuture<?> authTimeout;

    public Netty5ServerHandler(@NotNull Netty5Server server) {
        this.server = server;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Packet packet) {
        if (packet instanceof AuthPacket authPacket) {
            authenticate(ctx, authPacket);
            return;
        }

        var sender = this.clientChannel;
        if (sender == null) {
            System.err.println(prefix() + "Packet of unauthenticated channel rejected, closing " + ctx.channel().remoteAddress());
            ctx.close();
            return;
        }

        for (var filter : server.filters()) {
            if (filter instanceof PacketReceiveFilter packetReceiveFilter) {
                if (!packetReceiveFilter.evaluateFilter(new PacketReceiveFilter.FilterValue(
                        packet,
                        sender
                ))) {
                    System.err.println("Receiving packet not allowed by filter (" + sender.identity().name() + ";" + packet.getClass().getName() + ")");
                    return;
                }
            }
        }

        server.packetTransmitter().call(packet, sender);
    }

    private void authenticate(ChannelHandlerContext ctx, AuthPacket authPacket) {
        if (this.clientChannel != null) {
            System.err.println(prefix() + "Duplicate authentication ignored (" + authPacket.identity().name() + ")");
            return;
        }

        var netty5Channel = new Netty5ClientChannel(authPacket.identity(), ctx.channel(), null);
        for (var filter : server.filters()) {
            if (filter instanceof ConnectionFilter connectionFilter) {
                if (!connectionFilter.evaluateFilter(new Netty5ClientChannel.AuthType(netty5Channel, authPacket.properties()))) {
                    System.err.println("Connection not permitted due to filter (" + authPacket.identity().name() + ")");
                    ctx.close();
                    return;
                }
            }
        }

        netty5Channel.transmitter(new Netty5ClientPacketTransmitter(server.workerGroup(), netty5Channel::sendPacket));
        this.clientChannel = netty5Channel;
        ctx.channel().attr(Netty5ChannelUtils.AUTHENTICATED).set(Boolean.TRUE);
        cancelAuthTimeout();
        server.connections().add(netty5Channel);

        runActions(ConnectionAction.State.CLIENT_AUTHENTICATED, netty5Channel);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        var timeout = server.authTimeoutMillis();
        if (timeout > 0) {
            this.authTimeout = ctx.executor().schedule(() -> {
                if (this.clientChannel == null && ctx.channel().isActive()) {
                    System.err.println(prefix() + "Authentication timeout, closing " + ctx.channel().remoteAddress());
                    ctx.close();
                }
            }, timeout, TimeUnit.MILLISECONDS);
        }
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        cancelAuthTimeout();
        var channel = this.clientChannel;
        if (channel != null && server.connections().remove(channel)) {
            runActions(ConnectionAction.State.CLIENT_DISCONNECTED, channel);
        }
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        if (cause instanceof IOException) {
            return;
        }
        System.err.println(prefix() + "Exception caught: " + cause);
        if (cause instanceof TooLongFrameException
                || (cause instanceof PacketRejectedException && this.clientChannel == null)) {
            ctx.close();
        }
    }

    private void runActions(ConnectionAction.State state, Netty5ClientChannel channel) {
        for (var action : server.actions()) {
            if (action instanceof ConnectionAction connectionAction && connectionAction.state() == state) {
                try {
                    connectionAction.consumer().accept(channel);
                } catch (RuntimeException e) {
                    System.err.println(prefix() + "Connection action failed (" + state + "): " + e);
                }
            }
        }
    }

    private void cancelAuthTimeout() {
        var timeout = this.authTimeout;
        if (timeout != null) {
            timeout.cancel(false);
            this.authTimeout = null;
        }
    }

    private String prefix() {
        var identity = server.serverIdentity();
        return identity != null ? "[server: " + identity.name() + "] " : "";
    }
}

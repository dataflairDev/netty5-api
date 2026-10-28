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
import de.dataflair.netty5.common.codec.Netty5FrameDecoder;
import de.dataflair.netty5.common.codec.PacketRejectedException;
import de.dataflair.netty5.common.log.SafeLog;
import de.dataflair.netty5.common.packet.Packet;
import de.dataflair.netty5.common.packet.auth.AuthPacket;
import de.dataflair.netty5.filter.AsyncConnectionFilter;
import de.dataflair.netty5.filter.ConnectionFilter;
import de.dataflair.netty5.filter.PacketReceiveFilter;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.ScheduledFuture;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

/**
 * Server side handler. One instance per child channel; its state is only touched on the channel's event loop.
 * <p>
 * The first auth packet decides: it passes the synchronous {@link ConnectionFilter}s and then the
 * {@link AsyncConnectionFilter}s, or the channel is closed. Every later auth packet is ignored, and nothing is
 * processed any more once the channel has been closed or rejected.
 */
public final class Netty5ServerHandler extends SimpleChannelInboundHandler<Packet> {
    private static final Logger LOGGER = LoggerFactory.getLogger(Netty5ServerHandler.class);

    private enum State {NEW, PENDING, AUTHENTICATED, REJECTED}

    private final Netty5Server server;
    private State state = State.NEW;
    private Netty5ClientChannel clientChannel;
    private ScheduledFuture<?> authTimeout;
    private int rejectedPackets;

    public Netty5ServerHandler(@NotNull Netty5Server server) {
        this.server = server;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Packet packet) {
        if (!ctx.channel().isActive() || state == State.REJECTED) {
            return;
        }
        if (packet instanceof AuthPacket authPacket) {
            if (state == State.NEW) {
                authenticate(ctx, authPacket);
            } else {
                log(ctx, "Duplicate authentication ignored ({})", SafeLog.clean(authPacket.identity().name()));
            }
            return;
        }

        var sender = this.clientChannel;
        if (state != State.AUTHENTICATED || sender == null) {
            log(ctx, "Packet of unauthenticated channel rejected, closing {}", ctx.channel().remoteAddress());
            reject(ctx);
            return;
        }

        for (var filter : server.filters()) {
            if (filter instanceof PacketReceiveFilter packetReceiveFilter
                    && !packetReceiveFilter.evaluateFilter(new PacketReceiveFilter.FilterValue(packet, sender))) {
                log(ctx, "Receiving packet not allowed by filter ({}; {})", SafeLog.clean(sender.identity().name()),
                        packet.getClass().getName());
                countRejected(ctx);
                return;
            }
        }

        server.packetTransmitter().call(packet, sender);
    }

    private void authenticate(ChannelHandlerContext ctx, AuthPacket authPacket) {
        var name = SafeLog.clean(authPacket.identity().name());
        var address = address(ctx);
        if (address != null && server.authFailureLimiter().isBlocked(address)) {
            log(ctx, "Authentication of {} rejected, {} is blocked after failed authentications", name, address);
            reject(ctx);
            return;
        }

        var netty5Channel = new Netty5ClientChannel(authPacket.identity(), ctx.channel(), null);
        var auth = new Netty5ClientChannel.AuthType(netty5Channel, authPacket.properties());
        for (var filter : server.filters()) {
            if (filter instanceof ConnectionFilter connectionFilter) {
                final boolean accepted;
                try {
                    accepted = connectionFilter.evaluateFilter(auth);
                } catch (RuntimeException e) {
                    LOGGER.warn("{}Connection filter failed for {}: {}", server.prefix(), name, SafeLog.clean(e));
                    failAuthentication(ctx, name, address);
                    return;
                }
                if (!accepted) {
                    failAuthentication(ctx, name, address);
                    return;
                }
            }
        }

        var asyncFilters = server.asyncConnectionFilters();
        if (asyncFilters.isEmpty()) {
            completeAuthentication(ctx, netty5Channel, name);
            return;
        }

        state = State.PENDING;
        var channel = ctx.channel();
        channel.attr(Netty5ChannelUtils.AUTH_PENDING).set(Boolean.TRUE);
        Netty5ChannelUtils.suspendRead(channel, Netty5ChannelUtils.READ_SUSPENDED_AUTHENTICATION);

        CompletionStage<Boolean> result = CompletableFuture.completedFuture(Boolean.TRUE);
        for (var filter : asyncFilters) {
            result = result.thenCompose(accepted -> Boolean.TRUE.equals(accepted)
                    ? filter.evaluateAsync(auth)
                    : CompletableFuture.completedFuture(Boolean.FALSE));
        }
        result.whenComplete((accepted, throwable) -> ctx.executor().execute(() -> {
            if (throwable != null) {
                LOGGER.warn("{}Async connection filter failed for {}: {}", server.prefix(), name,
                        SafeLog.clean(throwable));
            }
            finishAsyncAuthentication(ctx, netty5Channel, name, address, throwable == null && Boolean.TRUE.equals(accepted));
        }));
    }

    private void finishAsyncAuthentication(ChannelHandlerContext ctx, Netty5ClientChannel netty5Channel, String name,
                                           @Nullable InetAddress address, boolean accepted) {
        var channel = ctx.channel();
        channel.attr(Netty5ChannelUtils.AUTH_PENDING).set(null);
        if (state != State.PENDING || !channel.isActive()) {
            return;
        }
        if (!accepted) {
            failAuthentication(ctx, name, address);
            return;
        }
        completeAuthentication(ctx, netty5Channel, name);
        if (state != State.AUTHENTICATED) {
            return;
        }
        Netty5ChannelUtils.resumeRead(channel, Netty5ChannelUtils.READ_SUSPENDED_AUTHENTICATION);
        var frameDecoder = channel.pipeline().get(Netty5FrameDecoder.class);
        var frameContext = channel.pipeline().context(Netty5FrameDecoder.class);
        if (frameDecoder != null && frameContext != null) {
            try {
                frameDecoder.resume(frameContext);
            } catch (Exception e) {
                ctx.fireExceptionCaught(e);
            }
        }
    }

    private void failAuthentication(ChannelHandlerContext ctx, String name, @Nullable InetAddress address) {
        log(ctx, "Connection not permitted due to filter ({})", name);
        if (address != null) {
            server.authFailureLimiter().recordFailure(address);
        }
        reject(ctx);
    }

    private void completeAuthentication(ChannelHandlerContext ctx, Netty5ClientChannel netty5Channel, String name) {
        netty5Channel.transmitter(new Netty5ClientPacketTransmitter(server.workerGroup(), netty5Channel::sendPacket));
        if (!server.register(netty5Channel)) {
            log(ctx, "Identity {} ({}) is already connected, closing the new connection", name,
                    netty5Channel.identity().uuid());
            reject(ctx);
            return;
        }
        this.clientChannel = netty5Channel;
        this.state = State.AUTHENTICATED;
        ctx.channel().attr(Netty5ChannelUtils.AUTHENTICATED).set(Boolean.TRUE);
        cancelAuthTimeout();
        var idle = server.idleTimeoutMillis();
        if (idle > 0) {
            ctx.pipeline().addBefore(ctx.name(), "idle", new IdleStateHandler(0, 0, idle, TimeUnit.MILLISECONDS));
        }

        runActions(ConnectionAction.State.CLIENT_AUTHENTICATED, netty5Channel);
    }

    private void reject(ChannelHandlerContext ctx) {
        state = State.REJECTED;
        cancelAuthTimeout();
        ctx.close();
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object event) throws Exception {
        if (event instanceof IdleStateEvent) {
            log(ctx, "Idle timeout, closing {}", ctx.channel().remoteAddress());
            ctx.close();
            return;
        }
        super.userEventTriggered(ctx, event);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        var timeout = server.authTimeoutMillis();
        if (timeout > 0) {
            this.authTimeout = ctx.executor().schedule(() -> {
                if (state != State.AUTHENTICATED && ctx.channel().isActive()) {
                    log(ctx, "Authentication timeout, closing {}", ctx.channel().remoteAddress());
                    reject(ctx);
                }
            }, timeout, TimeUnit.MILLISECONDS);
        }
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        cancelAuthTimeout();
        ctx.channel().attr(Netty5ChannelUtils.AUTH_PENDING).set(null);
        var channel = this.clientChannel;
        if (channel != null) {
            this.clientChannel = null;
            server.connections().remove(channel);
            runActions(ConnectionAction.State.CLIENT_DISCONNECTED, channel);
        }
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        if (cause instanceof IOException) {
            return;
        }
        log(ctx, "Exception caught: {}", SafeLog.clean(cause));
        // after a framing error the stream position is lost; a rejected packet only closes before authentication
        // or once too many packets were rejected
        if (cause instanceof PacketRejectedException && state == State.AUTHENTICATED && ctx.channel().isActive()) {
            countRejected(ctx);
            return;
        }
        if (cause instanceof DecoderException || state != State.AUTHENTICATED) {
            reject(ctx);
        }
    }

    private void countRejected(ChannelHandlerContext ctx) {
        if (++rejectedPackets >= server.maxRejectedPackets()) {
            log(ctx, "{} rejected packets, closing {}", rejectedPackets, ctx.channel().remoteAddress());
            ctx.close();
        }
    }

    private void runActions(ConnectionAction.State state, Netty5ClientChannel channel) {
        for (var action : server.actions()) {
            if (action instanceof ConnectionAction connectionAction && connectionAction.state() == state) {
                try {
                    connectionAction.consumer().accept(channel);
                } catch (RuntimeException e) {
                    LOGGER.warn("{}Connection action failed ({}): {}", server.prefix(), state, SafeLog.clean(e));
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

    private void log(ChannelHandlerContext ctx, String message, Object... arguments) {
        if (SafeLog.allow(ctx.channel())) {
            LOGGER.warn(server.prefix() + message, arguments);
        }
    }

    private static @Nullable InetAddress address(ChannelHandlerContext ctx) {
        return ctx.channel().remoteAddress() instanceof InetSocketAddress socketAddress
                ? socketAddress.getAddress() : null;
    }
}

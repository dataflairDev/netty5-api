package de.dataflair.netty.server;

/*
 * Copyright 2023-2024 netty-api contributors
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

import de.dataflair.netty.NettyChannelUtils;
import de.dataflair.netty.NettyClientChannel;
import de.dataflair.netty.actions.ConnectionAction;
import de.dataflair.netty.client.NettyClientPacketTransmitter;
import de.dataflair.netty.common.codec.NettyFrameDecoder;
import de.dataflair.netty.common.codec.PacketRejectedException;
import de.dataflair.netty.common.log.SafeLog;
import de.dataflair.netty.common.packet.Packet;
import de.dataflair.netty.common.packet.auth.AuthAcceptedPacket;
import de.dataflair.netty.common.packet.auth.AuthPacket;
import de.dataflair.netty.common.packet.auth.KeepAlivePacket;
import de.dataflair.netty.filter.AsyncConnectionFilter;
import de.dataflair.netty.filter.ConnectionFilter;
import de.dataflair.netty.filter.PacketReceiveFilter;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.ScheduledFuture;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

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
 * {@link AsyncConnectionFilter}s, or the channel is closed. Every attempt takes a token of the
 * {@link AuthFailureLimiter} when it starts and gives it back on success. A later auth packet counts as a rejected
 * packet, and nothing is processed any more once the channel has been closed or rejected. After authentication
 * the server answers with an {@link AuthAcceptedPacket} carrying the effective identity, and drops
 * {@link KeepAlivePacket}s before any filter or listener.
 */
@Slf4j
public final class NettyServerHandler extends SimpleChannelInboundHandler<Packet> {

    private enum State {NEW, PENDING, AUTHENTICATED, REJECTED}

    private final NettyServer server;
    private State state = State.NEW;
    private NettyClientChannel clientChannel;
    private ScheduledFuture<?> authTimeout;
    private int rejectedPackets;

    public NettyServerHandler(@NotNull NettyServer server) {
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
                log(ctx, "Duplicate authentication rejected ({})", SafeLog.clean(authPacket.identity().name()));
                countRejected(ctx);
            }
            return;
        }

        var sender = this.clientChannel;
        if (state != State.AUTHENTICATED || sender == null) {
            log(ctx, "Packet of unauthenticated channel rejected, closing {}", ctx.channel().remoteAddress());
            reject(ctx);
            return;
        }
        if (packet instanceof KeepAlivePacket) {
            // only resets the reader idle timeout
            return;
        }
        if (packet instanceof AuthAcceptedPacket) {
            log(ctx, "AuthAcceptedPacket from a client rejected ({})", SafeLog.clean(sender.identity().name()));
            countRejected(ctx);
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
        // the attempt is counted before any filter runs, so parallel attempts cannot all pass the check
        if (address != null && !server.authFailureLimiter().tryAcquire(address)) {
            log(ctx, "Authentication of {} rejected, {} is blocked after failed authentications", name, address);
            reject(ctx);
            return;
        }

        var identity = server.effectiveIdentity(authPacket.identity());
        var nettyChannel = new NettyClientChannel(identity, ctx.channel(), null);
        var auth = new NettyClientChannel.AuthType(nettyChannel, authPacket.properties());
        for (var filter : server.filters()) {
            if (filter instanceof ConnectionFilter connectionFilter) {
                final boolean accepted;
                try {
                    accepted = connectionFilter.evaluateFilter(auth);
                } catch (RuntimeException e) {
                    log.warn("{}Connection filter failed for {}: {}", server.prefix(), name, SafeLog.clean(e));
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
            completeAuthentication(ctx, nettyChannel, name, address);
            return;
        }

        state = State.PENDING;
        var channel = ctx.channel();
        channel.attr(NettyChannelUtils.AUTH_PENDING).set(Boolean.TRUE);
        NettyChannelUtils.suspendRead(channel, NettyChannelUtils.READ_SUSPENDED_AUTHENTICATION);

        CompletionStage<Boolean> result = CompletableFuture.completedFuture(Boolean.TRUE);
        for (var filter : asyncFilters) {
            result = result.thenCompose(accepted -> Boolean.TRUE.equals(accepted)
                    ? filter.evaluateAsync(auth)
                    : CompletableFuture.completedFuture(Boolean.FALSE));
        }
        result.whenComplete((accepted, throwable) -> ctx.executor().execute(() -> {
            if (throwable != null) {
                log.warn("{}Async connection filter failed for {}: {}", server.prefix(), name,
                        SafeLog.clean(throwable));
            }
            finishAsyncAuthentication(ctx, nettyChannel, name, address, throwable == null && Boolean.TRUE.equals(accepted));
        }));
    }

    private void finishAsyncAuthentication(ChannelHandlerContext ctx, NettyClientChannel nettyChannel, String name,
                                           @Nullable InetAddress address, boolean accepted) {
        var channel = ctx.channel();
        channel.attr(NettyChannelUtils.AUTH_PENDING).set(null);
        if (state != State.PENDING || !channel.isActive()) {
            return;
        }
        if (!accepted) {
            failAuthentication(ctx, name, address);
            return;
        }
        completeAuthentication(ctx, nettyChannel, name, address);
        if (state != State.AUTHENTICATED) {
            return;
        }
        NettyChannelUtils.resumeRead(channel, NettyChannelUtils.READ_SUSPENDED_AUTHENTICATION);
        var frameDecoder = channel.pipeline().get(NettyFrameDecoder.class);
        var frameContext = channel.pipeline().context(NettyFrameDecoder.class);
        if (frameDecoder != null && frameContext != null) {
            try {
                frameDecoder.resume(frameContext);
            } catch (Exception e) {
                ctx.fireExceptionCaught(e);
            }
        }
    }

    private void failAuthentication(ChannelHandlerContext ctx, String name, @Nullable InetAddress address) {
        // the token taken in authenticate() is not given back
        log(ctx, "Connection not permitted due to filter ({})", name);
        reject(ctx);
    }

    private void completeAuthentication(ChannelHandlerContext ctx, NettyClientChannel nettyChannel, String name,
                                        @Nullable InetAddress address) {
        if (address != null) {
            // the filters accepted: this attempt is no failure
            server.authFailureLimiter().release(address);
        }
        nettyChannel.transmitter(new NettyClientPacketTransmitter(server.workerGroup(), nettyChannel::sendPacket));
        if (!server.register(nettyChannel)) {
            log(ctx, "Identity {} ({}) is already connected, closing the new connection", name,
                    nettyChannel.identity().uuid());
            reject(ctx);
            return;
        }
        this.clientChannel = nettyChannel;
        this.state = State.AUTHENTICATED;
        ctx.channel().attr(NettyChannelUtils.AUTHENTICATED).set(Boolean.TRUE);
        cancelAuthTimeout();
        var limiter = server.connectionLimiter();
        if (limiter != null) {
            limiter.authenticated(ctx.channel());
        }
        var idle = server.idleTimeoutMillis();
        if (idle > 0) {
            // reader idle: our own writes must not keep a peer alive that no longer sends anything
            ctx.pipeline().addBefore(ctx.name(), "idle", new IdleStateHandler(idle, 0, 0, TimeUnit.MILLISECONDS));
        }
        // the first packet the client receives tells it the identity the server uses
        ctx.channel().writeAndFlush(new AuthAcceptedPacket(nettyChannel.identity()));

        runActions(ConnectionAction.State.CLIENT_AUTHENTICATED, nettyChannel);
    }

    private void reject(ChannelHandlerContext ctx) {
        state = State.REJECTED;
        cancelAuthTimeout();
        ctx.close();
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object event) throws Exception {
        if (event instanceof IdleStateEvent) {
            log(ctx, "Nothing received within the idle timeout, closing {}", ctx.channel().remoteAddress());
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
        ctx.channel().attr(NettyChannelUtils.AUTH_PENDING).set(null);
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

    private void runActions(ConnectionAction.State state, NettyClientChannel channel) {
        for (var action : server.actions()) {
            if (action instanceof ConnectionAction connectionAction && connectionAction.state() == state) {
                try {
                    connectionAction.consumer().accept(channel);
                } catch (RuntimeException e) {
                    log.warn("{}Connection action failed ({}): {}", server.prefix(), state, SafeLog.clean(e));
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
            log.warn(server.prefix() + message, arguments);
        }
    }

    private static @Nullable InetAddress address(ChannelHandlerContext ctx) {
        return ctx.channel().remoteAddress() instanceof InetSocketAddress socketAddress
                ? socketAddress.getAddress() : null;
    }
}

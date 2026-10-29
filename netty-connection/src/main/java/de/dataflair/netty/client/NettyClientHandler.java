package de.dataflair.netty.client;



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

import de.dataflair.netty.NettyClientChannel;
import de.dataflair.netty.NettyComponent;
import de.dataflair.netty.common.codec.PacketRejectedException;
import de.dataflair.netty.common.log.SafeLog;
import de.dataflair.netty.common.packet.Packet;
import de.dataflair.netty.common.packet.auth.AuthAcceptedPacket;
import de.dataflair.netty.common.packet.auth.AuthPacket;
import de.dataflair.netty.common.packet.auth.KeepAlivePacket;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Client side handler. Sends the {@link AuthPacket} when the connection is up, with TLS only after the handshake
 * has succeeded. Consumes the {@link AuthAcceptedPacket} of the server (effective identity, starts the keep-alive)
 * and drops {@link KeepAlivePacket}s. After {@code maxRejectedPackets} (default 16) rejected packets the
 * connection is closed (and re-established by the client).
 */
@Slf4j
public final class NettyClientHandler extends SimpleChannelInboundHandler<Packet> {

    private final NettyClient client;
    private int rejectedPackets;
    private boolean accepted;

    public NettyClientHandler(@NotNull NettyClient client) {
        this.client = client;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Packet packet) {
        if (!ctx.channel().isActive()) {
            return;
        }
        if (packet instanceof AuthAcceptedPacket acceptedPacket) {
            if (!accepted) {
                accepted(ctx, acceptedPacket);
            }
            return;
        }
        if (packet instanceof KeepAlivePacket) {
            return;
        }
        client.packetTransmitter().call(packet, null);
    }

    private void accepted(ChannelHandlerContext ctx, AuthAcceptedPacket packet) {
        accepted = true;
        var identity = packet.identity();
        client.effectiveIdentity(identity);
        var channel = client.thisChannel();
        if (channel != null && channel.channel() == ctx.channel()) {
            channel.identity(identity);
        }
        var interval = client.keepAliveIntervalMillis();
        if (interval > 0 && ctx.pipeline().get("keep-alive") == null) {
            ctx.pipeline().addBefore(ctx.name(), "keep-alive",
                    new IdleStateHandler(0, interval, 0, TimeUnit.MILLISECONDS));
        }
        client.authenticationFuture().complete(identity);
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object event) throws Exception {
        if (event instanceof IdleStateEvent idle && idle.state() == IdleState.WRITER_IDLE) {
            if (ctx.channel().isActive()) {
                ctx.channel().writeAndFlush(new KeepAlivePacket());
            }
            return;
        }
        super.userEventTriggered(ctx, event);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        var sslHandler = ctx.pipeline().get(SslHandler.class);
        if (sslHandler == null) {
            authenticate(ctx);
        } else {
            sslHandler.handshakeFuture().addListener(future -> {
                if (future.isSuccess()) {
                    authenticate(ctx);
                } else {
                    log.warn("[client: {}] TLS handshake failed: {}", client.identity().name(),
                            SafeLog.clean(future.cause()));
                    ctx.close();
                }
            });
        }
        super.channelActive(ctx);
    }

    private void authenticate(ChannelHandlerContext ctx) {
        if (!ctx.channel().isActive()) {
            return;
        }
        ctx.channel().writeAndFlush(new AuthPacket(client.identity(), client.authProperty()));
        client.thisChannel(new NettyClientChannel(client.identity(), ctx.channel(), client.packetTransmitter()));
        client.connectionState(NettyComponent.ConnectionState.CONNECTED);
        client.connectionFuture().complete(null);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        client.connectionLost();
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        if (cause instanceof IOException) {
            return;
        }
        if (SafeLog.allow(ctx.channel())) {
            log.warn("[client: {}] Exception caught: {}", client.identity().name(), SafeLog.clean(cause));
        }
        // after a framing error the stream position is lost; a rejected packet is skipped
        if (cause instanceof PacketRejectedException) {
            if (++rejectedPackets >= client.maxRejectedPackets()) {
                ctx.close();
            }
            return;
        }
        if (cause instanceof DecoderException) {
            ctx.close();
        }
    }
}

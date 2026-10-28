package de.dataflair.netty5.client;



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
import de.dataflair.netty5.Netty5Component;
import de.dataflair.netty5.common.codec.PacketRejectedException;
import de.dataflair.netty5.common.log.SafeLog;
import de.dataflair.netty5.common.packet.Packet;
import de.dataflair.netty5.common.packet.auth.AuthPacket;
import de.dataflair.netty5.server.Netty5Server;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.ssl.SslHandler;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Client side handler. Sends the {@link AuthPacket} when the connection is up, with TLS only after the handshake
 * has succeeded. After {@link Netty5Server#DEFAULT_MAX_REJECTED_PACKETS} rejected packets the connection is
 * closed (and re-established by the client).
 */
public final class Netty5ClientHandler extends SimpleChannelInboundHandler<Packet> {
    private static final Logger LOGGER = LoggerFactory.getLogger(Netty5ClientHandler.class);

    private final Netty5Client client;
    private int rejectedPackets;

    public Netty5ClientHandler(@NotNull Netty5Client client) {
        this.client = client;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Packet packet) {
        if (!ctx.channel().isActive()) {
            return;
        }
        client.packetTransmitter().call(packet, null);
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
                    LOGGER.warn("[client: {}] TLS handshake failed: {}", client.identity().name(),
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
        client.thisChannel(new Netty5ClientChannel(client.identity(), ctx.channel(), client.packetTransmitter()));
        client.connectionState(Netty5Component.ConnectionState.CONNECTED);
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
            LOGGER.warn("[client: {}] Exception caught: {}", client.identity().name(), SafeLog.clean(cause));
        }
        // after a framing error the stream position is lost; a rejected packet is skipped
        if (cause instanceof PacketRejectedException) {
            if (++rejectedPackets >= Netty5Server.DEFAULT_MAX_REJECTED_PACKETS) {
                ctx.close();
            }
            return;
        }
        if (cause instanceof DecoderException) {
            ctx.close();
        }
    }
}

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
import de.dataflair.netty5.common.packet.Packet;
import de.dataflair.netty5.common.packet.auth.AuthPacket;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.TooLongFrameException;
import lombok.AllArgsConstructor;

import java.io.IOException;

@AllArgsConstructor
public final class Netty5ClientHandler extends SimpleChannelInboundHandler<Packet> {
    private final Netty5Client client;

    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, Packet packet) {
        client.packetTransmitter().call(packet, null);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        ctx.channel().writeAndFlush(new AuthPacket(client.identity(), client.authProperty()));
        client.thisChannel(new Netty5ClientChannel(client.identity(), ctx.channel(), client.packetTransmitter()));
        client.connectionState(Netty5Component.ConnectionState.CONNECTED);
        client.connectionFuture().complete(null);
        super.channelActive(ctx);
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
        System.err.println("[client: " + client.identity().name() + "] Exception caught: " + cause);
        if (cause instanceof TooLongFrameException) {
            ctx.close();
        }
    }
}

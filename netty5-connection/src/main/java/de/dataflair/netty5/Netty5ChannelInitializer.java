package de.dataflair.netty5;



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

import de.dataflair.netty5.common.codec.PacketDecoder;
import de.dataflair.netty5.common.codec.PacketEncoder;
import de.dataflair.netty5.common.codec.PacketRegistry;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public abstract class Netty5ChannelInitializer extends ChannelInitializer<Channel> {
    private final Netty5ClientChannel.Identity identity;
    private final int maxFrameLength;
    private final PacketRegistry packetRegistry;
    private final boolean requireAuthentication;

    public Netty5ChannelInitializer(Netty5ClientChannel.Identity identity) {
        this(identity, Netty5ChannelUtils.DEFAULT_MAX_FRAME_LENGTH, null, false);
    }

    /**
     * @param identity              identity used for log messages, may be null
     * @param maxFrameLength        maximum accepted frame length in bytes; larger frames are rejected
     * @param packetRegistry        registry used to resolve incoming packet classes, null for the default registry
     * @param requireAuthentication if true only the auth packet is accepted until the channel is authenticated
     */
    public Netty5ChannelInitializer(@Nullable Netty5ClientChannel.Identity identity,
                                    int maxFrameLength,
                                    @Nullable PacketRegistry packetRegistry,
                                    boolean requireAuthentication) {
        if (maxFrameLength <= 0) {
            throw new IllegalArgumentException("maxFrameLength must be positive: " + maxFrameLength);
        }
        this.identity = identity;
        this.maxFrameLength = maxFrameLength;
        this.packetRegistry = packetRegistry == null ? new PacketRegistry() : packetRegistry;
        this.requireAuthentication = requireAuthentication;
    }

    @Override
    protected void initChannel(@NotNull Channel channel) {
        channel.pipeline()
                .addLast(new LengthFieldBasedFrameDecoder(maxFrameLength, 0, Integer.BYTES, 0, Integer.BYTES))
                .addLast(new PacketDecoder(identity, packetRegistry, requireAuthentication))
                .addLast(new LengthFieldPrepender(Integer.BYTES))
                .addLast(new PacketEncoder(identity))
                .addLast(handler());
    }

    public abstract SimpleChannelInboundHandler<?> handler();
}

package de.dataflair.netty;



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

import de.dataflair.netty.common.codec.PacketDecoder;
import de.dataflair.netty.common.codec.PacketEncoder;
import de.dataflair.netty.common.codec.PacketRegistry;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.SimpleChannelInboundHandler;
import de.dataflair.netty.common.codec.NettyFrameDecoder;
import io.netty.channel.ChannelHandler;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.ssl.SslHandler;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.function.Function;

public abstract class NettyChannelInitializer extends ChannelInitializer<Channel> {
    private final NettyClientChannel.Identity identity;
    private final int maxFrameLength;
    private final int preAuthMaxFrameLength;
    private final PacketRegistry packetRegistry;
    private final boolean requireAuthentication;
    private @Nullable ChannelHandler firstHandler;
    private @Nullable Function<Channel, SslHandler> sslHandlerFactory;
    private long maxOutboundBufferBytes = NettyComponent.DEFAULT_MAX_OUTBOUND_BUFFER_BYTES;
    private boolean suspendReadWhenUnwritable = true;

    public NettyChannelInitializer(NettyClientChannel.Identity identity) {
        this(identity, NettyChannelUtils.DEFAULT_MAX_FRAME_LENGTH, null, false);
    }

    /**
     * Uses {@link NettyChannelUtils#DEFAULT_PRE_AUTH_MAX_FRAME_LENGTH} as frame limit before authentication.
     *
     * @see #NettyChannelInitializer(NettyClientChannel.Identity, int, int, PacketRegistry, boolean)
     */
    public NettyChannelInitializer(@Nullable NettyClientChannel.Identity identity,
                                    int maxFrameLength,
                                    @Nullable PacketRegistry packetRegistry,
                                    boolean requireAuthentication) {
        this(identity, maxFrameLength, NettyChannelUtils.DEFAULT_PRE_AUTH_MAX_FRAME_LENGTH, packetRegistry,
                requireAuthentication);
    }

    /**
     * @param identity              identity used for log messages, may be null
     * @param maxFrameLength        maximum frame length in bytes (including the length field); larger incoming
     *                              frames are rejected, larger outgoing packets fail to encode
     * @param preAuthMaxFrameLength maximum incoming frame length in bytes (including the length field) while the
     *                              channel is not authenticated; only used if {@code requireAuthentication} is set
     * @param packetRegistry        registry used to resolve incoming packet classes, null for the default registry
     * @param requireAuthentication if true only the auth packet is accepted until the channel is authenticated
     */
    public NettyChannelInitializer(@Nullable NettyClientChannel.Identity identity,
                                    int maxFrameLength,
                                    int preAuthMaxFrameLength,
                                    @Nullable PacketRegistry packetRegistry,
                                    boolean requireAuthentication) {
        if (maxFrameLength <= 0) {
            throw new IllegalArgumentException("maxFrameLength must be positive: " + maxFrameLength);
        }
        if (preAuthMaxFrameLength <= 0) {
            throw new IllegalArgumentException("preAuthMaxFrameLength must be positive: " + preAuthMaxFrameLength);
        }
        this.identity = identity;
        this.maxFrameLength = maxFrameLength;
        this.preAuthMaxFrameLength = preAuthMaxFrameLength;
        this.packetRegistry = packetRegistry == null ? new PacketRegistry() : packetRegistry;
        this.requireAuthentication = requireAuthentication;
    }

    /**
     * @param handler a (sharable) handler placed in front of every other handler, for example a connection
     *                limiter; null for none
     */
    public NettyChannelInitializer firstHandler(@Nullable ChannelHandler handler) {
        this.firstHandler = handler;
        return this;
    }

    /**
     * @param factory creates the {@link SslHandler} of a channel; null for plain TCP
     */
    public NettyChannelInitializer sslHandlerFactory(@Nullable Function<Channel, SslHandler> factory) {
        this.sslHandlerFactory = factory;
        return this;
    }

    /**
     * @param bytes hard limit of the outbound buffer, see {@link BackpressureHandler}
     */
    public NettyChannelInitializer maxOutboundBufferBytes(long bytes) {
        if (bytes <= 0) {
            throw new IllegalArgumentException("maxOutboundBufferBytes must be positive: " + bytes);
        }
        this.maxOutboundBufferBytes = bytes;
        return this;
    }

    /**
     * @param suspend true (default) to stop reading while the channel is unwritable; the client passes false, see
     *                {@link BackpressureHandler}
     */
    public NettyChannelInitializer suspendReadWhenUnwritable(boolean suspend) {
        this.suspendReadWhenUnwritable = suspend;
        return this;
    }

    @Override
    protected void initChannel(@NotNull Channel channel) {
        var pipeline = channel.pipeline();
        if (firstHandler != null) {
            pipeline.addLast("connection-limit", firstHandler);
        }
        if (sslHandlerFactory != null) {
            pipeline.addLast("ssl", sslHandlerFactory.apply(channel));
        }
        pipeline.addLast("backpressure", new BackpressureHandler(maxOutboundBufferBytes, suspendReadWhenUnwritable))
                .addLast("frame-decoder", new NettyFrameDecoder(maxFrameLength, preAuthMaxFrameLength,
                        requireAuthentication))
                .addLast("packet-decoder", new PacketDecoder(identity, packetRegistry, requireAuthentication,
                        preAuthMaxFrameLength))
                .addLast("frame-prepender", new LengthFieldPrepender(Integer.BYTES))
                .addLast("packet-encoder", new PacketEncoder(identity, maxFrameLength))
                .addLast("handler", handler());
    }

    public abstract SimpleChannelInboundHandler<?> handler();
}

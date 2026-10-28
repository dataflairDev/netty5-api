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

import de.dataflair.netty5.Netty5ChannelInitializer;
import de.dataflair.netty5.Netty5ChannelUtils;
import de.dataflair.netty5.Netty5ClientChannel;
import de.dataflair.netty5.Netty5Component;
import de.dataflair.netty5.filter.Filter;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.concurrent.Future;
import lombok.Getter;
import lombok.Setter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

@Getter
public final class Netty5Server extends Netty5Component {
    /**
     * Default time in milliseconds a connection may stay unauthenticated before it is closed.
     */
    public static final long DEFAULT_AUTH_TIMEOUT_MILLIS = 10_000;

    private final EventLoopGroup workerGroup = Netty5ChannelUtils.createEventLoopGroup(0);
    private final List<Filter<?>> filters = new CopyOnWriteArrayList<>();
    private final Netty5ServerPacketTransmitter packetTransmitter;
    /**
     * Authenticated connections. Thread-safe; entries are added after a successful authentication and removed
     * when the channel becomes inactive.
     */
    private final List<Netty5ClientChannel> connections = new CopyOnWriteArrayList<>();
    @Setter
    private volatile Netty5ClientChannel.Identity serverIdentity;
    /**
     * Time in milliseconds a connection may stay unauthenticated before it is closed; 0 or less disables the
     * timeout. Must be set before {@link #initialize()}.
     */
    @Setter
    private volatile long authTimeoutMillis = DEFAULT_AUTH_TIMEOUT_MILLIS;
    private volatile Channel serverChannel;

    public Netty5Server(@NotNull String hostname, int port) {
        super(1, hostname, port);
        this.packetTransmitter = new Netty5ServerPacketTransmitter(bossGroup(), packet -> {

        }, (requestPacket, packetClass, consumer) -> {

        });
    }

    public <T extends Filter<?>> Netty5Server addFilter(@NotNull T filter) {
        this.filters.add(filter);
        return this;
    }

    @Override
    public void initialize() throws Exception {
        var initializer = new Netty5ChannelInitializer(this.serverIdentity, maxFrameLength(), packetRegistry(), true) {
            @Override
            public SimpleChannelInboundHandler<?> handler() {
                return new Netty5ServerHandler(Netty5Server.this);
            }
        };
        new ServerBootstrap()
                .group(bossGroup(), workerGroup)
                .channelFactory(Netty5ChannelUtils.buildChannelFactory())
                .childHandler(initializer)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childOption(ChannelOption.IP_TOS, 24)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .bind(this.hostname(), this.port())
                .addListener((ChannelFutureListener) future -> {
                    if (future.isSuccess()) {
                        this.serverChannel = future.channel();
                    } else {
                        System.err.println((serverIdentity != null ? "[server: " + serverIdentity.name() + "] " : "")
                                + "Bind failed: " + future.cause());
                    }
                })
                .addListener(deployFuture());
    }

    /**
     * @return the address the server is bound to, or null if it is not bound (yet)
     */
    public @Nullable InetSocketAddress localAddress() {
        var channel = this.serverChannel;
        return channel == null ? null : (InetSocketAddress) channel.localAddress();
    }

    @Override
    public Future<?> shutdownGracefully(long quietPeriod, long timeout, @NotNull TimeUnit unit) {
        var channel = this.serverChannel;
        if (channel != null) {
            channel.close();
        }
        this.workerGroup.shutdownGracefully(quietPeriod, timeout, unit);
        return super.shutdownGracefully(quietPeriod, timeout, unit);
    }

    public List<Netty5ClientChannel> clientChannel(@NotNull String name) {
        return this.connections.stream()
                .filter(clientChannel -> clientChannel.identity().name().equalsIgnoreCase(name))
                .toList();
    }

    public Netty5ClientChannel firstChannel(@NotNull String name) {
        return this.clientChannel(name).stream().findFirst().orElse(null);
    }

    public Netty5ClientChannel clientChannel(@NotNull UUID uuid) {
        return this.connections.stream()
                .filter(clientChannel -> uuid.equals(clientChannel.identity().uuid()))
                .findFirst()
                .orElse(null);
    }

}

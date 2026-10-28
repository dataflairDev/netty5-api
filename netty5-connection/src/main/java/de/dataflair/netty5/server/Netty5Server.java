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
import de.dataflair.netty5.filter.AsyncConnectionFilter;
import de.dataflair.netty5.filter.ConnectionFilter;
import de.dataflair.netty5.filter.Filter;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.netty.util.concurrent.Future;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

/**
 * TCP server. Fail-closed by default:
 * <ul>
 *     <li>{@link #initialize()} refuses to start without a {@link ConnectionFilter} or
 *     {@link AsyncConnectionFilter}, unless {@code allowAnonymous(true)} is set.</li>
 *     <li>Connections are limited globally ({@link #maxConnections}) and per remote address
 *     ({@link #maxConnectionsPerAddress}); failed authentications are throttled per address
 *     ({@link #maxAuthFailuresPerMinute}).</li>
 *     <li>A second connection with the name or UUID of an authenticated one is handled by the
 *     {@link #identityPolicy} (default {@link IdentityPolicy#REJECT}).</li>
 *     <li>Authenticated connections are closed after {@link #idleTimeoutMillis} without traffic and after
 *     {@link #maxRejectedPackets} rejected packets.</li>
 * </ul>
 * Filters, listeners, responders and actions run on the event loop of the connection and must not block.
 */
@Getter
public final class Netty5Server extends Netty5Component {
    /**
     * Default time in milliseconds a connection may stay unauthenticated before it is closed.
     */
    public static final long DEFAULT_AUTH_TIMEOUT_MILLIS = 10_000;
    /**
     * Default idle timeout of an authenticated connection (5 minutes).
     */
    public static final long DEFAULT_IDLE_TIMEOUT_MILLIS = 5 * 60_000;
    public static final int DEFAULT_MAX_CONNECTIONS = 10_000;
    public static final int DEFAULT_MAX_CONNECTIONS_PER_ADDRESS = 64;
    public static final int DEFAULT_MAX_AUTH_FAILURES_PER_MINUTE = 10;
    public static final int DEFAULT_MAX_REJECTED_PACKETS = 16;
    public static final int DEFAULT_BACKLOG = 1024;

    private static final Logger LOGGER = LoggerFactory.getLogger(Netty5Server.class);

    private final EventLoopGroup workerGroup = Netty5ChannelUtils.createEventLoopGroup(0);
    private final List<Filter<?>> filters = new CopyOnWriteArrayList<>();
    private final List<AsyncConnectionFilter> asyncConnectionFilters = new CopyOnWriteArrayList<>();
    private final Netty5ServerPacketTransmitter packetTransmitter;
    /**
     * Authenticated connections. Thread-safe; entries are added after a successful authentication and removed
     * when the channel becomes inactive.
     */
    private final List<Netty5ClientChannel> connections = new CopyOnWriteArrayList<>();
    @Getter(AccessLevel.NONE)
    private final Object identityLock = new Object();
    @Setter
    private volatile Netty5ClientChannel.Identity serverIdentity;
    /**
     * Time in milliseconds a connection may stay unauthenticated before it is closed; 0 or less disables the
     * timeout (logged as a warning). Must be set before {@link #initialize()}.
     */
    @Setter
    private volatile long authTimeoutMillis = DEFAULT_AUTH_TIMEOUT_MILLIS;
    /**
     * Time in milliseconds an authenticated connection may stay without reading or writing before it is closed;
     * 0 disables the timeout (logged as a warning). Must be set before {@link #initialize()}.
     */
    @Setter
    private volatile long idleTimeoutMillis = DEFAULT_IDLE_TIMEOUT_MILLIS;
    /**
     * If true the server starts without any connection filter and accepts every auth packet. Off by default.
     */
    @Setter
    private volatile boolean allowAnonymous;
    /**
     * Maximum number of open connections (authenticated or not). Must be set before {@link #initialize()}.
     */
    @Setter
    private volatile int maxConnections = DEFAULT_MAX_CONNECTIONS;
    /**
     * Maximum number of open connections per remote address. Must be set before {@link #initialize()}.
     */
    @Setter
    private volatile int maxConnectionsPerAddress = DEFAULT_MAX_CONNECTIONS_PER_ADDRESS;
    /**
     * Failed authentications per minute and remote address before the address is rejected; 0 or less disables
     * the throttling. Must be set before {@link #initialize()}.
     */
    @Setter
    private volatile int maxAuthFailuresPerMinute = DEFAULT_MAX_AUTH_FAILURES_PER_MINUTE;
    /**
     * Rejected packets (decode errors, unknown classes, {@link de.dataflair.netty5.filter.PacketReceiveFilter})
     * after which an authenticated connection is closed.
     */
    @Setter
    private volatile int maxRejectedPackets = DEFAULT_MAX_REJECTED_PACKETS;
    /**
     * Accept queue length ({@code SO_BACKLOG}). Must be set before {@link #initialize()}.
     */
    @Setter
    private volatile int backlog = DEFAULT_BACKLOG;
    /**
     * Policy for a duplicate name or UUID, default {@link IdentityPolicy#REJECT}.
     */
    @Setter
    private volatile @NotNull IdentityPolicy identityPolicy = IdentityPolicy.REJECT;
    private volatile AuthFailureLimiter authFailureLimiter = new AuthFailureLimiter(0);
    private volatile ConnectionLimiter connectionLimiter;
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

    /**
     * Adds a filter that decides asynchronously. It runs after all synchronous {@link ConnectionFilter}s.
     */
    public Netty5Server addFilter(@NotNull AsyncConnectionFilter filter) {
        this.asyncConnectionFilters.add(filter);
        return this;
    }

    /**
     * Binds the server.
     *
     * @throws IllegalStateException    if no connection filter is registered and anonymous access is not allowed
     * @throws IllegalArgumentException if a setting is invalid
     */
    @Override
    public void initialize() throws Exception {
        var hasConnectionFilter = !asyncConnectionFilters.isEmpty()
                || filters.stream().anyMatch(filter -> filter instanceof ConnectionFilter);
        if (!hasConnectionFilter && !allowAnonymous) {
            throw new IllegalStateException("No ConnectionFilter registered. Add a filter that checks the auth "
                    + "packet, or call allowAnonymous(true) to accept every client.");
        }
        if (!hasConnectionFilter) {
            LOGGER.warn("{}Anonymous access allowed: every client is accepted without a connection filter", prefix());
        }
        var ssl = sslContext();
        if (ssl != null && ssl.isClient()) {
            throw new IllegalArgumentException("The server needs a server SslContext");
        }
        if (idleTimeoutMillis < 0) {
            throw new IllegalArgumentException("idleTimeoutMillis must not be negative: " + idleTimeoutMillis);
        }
        if (idleTimeoutMillis == 0) {
            LOGGER.warn("{}Idle timeout disabled: authenticated connections are never closed for inactivity",
                    prefix());
        }
        var authTimeout = authTimeoutMillis;
        if (authTimeout <= 0) {
            LOGGER.warn("{}Authentication timeout disabled: unauthenticated connections stay open until the peer "
                    + "closes them", prefix());
        }
        if (ssl == null && !isLoopback(hostname())) {
            LOGGER.warn("{}Binding {} without TLS: identities and auth properties are sent in plain text",
                    prefix(), hostname());
        }

        var limiter = new AuthFailureLimiter(maxAuthFailuresPerMinute);
        this.authFailureLimiter = limiter;
        this.connectionLimiter = new ConnectionLimiter(maxConnections, maxConnectionsPerAddress, limiter);
        var handshakeTimeout = authTimeout > 0 ? Math.min(handshakeTimeoutMillis(), authTimeout)
                : handshakeTimeoutMillis();

        var initializer = new Netty5ChannelInitializer(this.serverIdentity, maxFrameLength(), preAuthMaxFrameLength(),
                packetRegistry(), true) {
            @Override
            public SimpleChannelInboundHandler<?> handler() {
                return new Netty5ServerHandler(Netty5Server.this);
            }
        };
        initializer.firstHandler(connectionLimiter).maxOutboundBufferBytes(maxOutboundBufferBytes());
        if (ssl != null) {
            initializer.sslHandlerFactory(channel -> {
                var handler = ssl.newHandler(channel.alloc());
                handler.setHandshakeTimeoutMillis(handshakeTimeout);
                return handler;
            });
        }
        new ServerBootstrap()
                .group(bossGroup(), workerGroup)
                .channelFactory(Netty5ChannelUtils.buildChannelFactory())
                .option(ChannelOption.SO_BACKLOG, backlog)
                .childHandler(initializer)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childOption(ChannelOption.IP_TOS, 24)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childOption(ChannelOption.WRITE_BUFFER_WATER_MARK, writeBufferWaterMark())
                .bind(this.hostname(), this.port())
                .addListener((ChannelFutureListener) future -> {
                    if (future.isSuccess()) {
                        this.serverChannel = future.channel();
                    } else {
                        LOGGER.error("{}Bind failed: {}", prefix(), String.valueOf(future.cause()));
                    }
                })
                .addListener(deployFuture());
    }

    /**
     * Adds an authenticated connection, applying the {@link #identityPolicy}.
     *
     * @return false if the connection was rejected because its identity is in use
     */
    boolean register(@NotNull Netty5ClientChannel channel) {
        var identity = channel.identity();
        synchronized (identityLock) {
            var conflicts = connections.stream()
                    .filter(existing -> existing.identity().name().equalsIgnoreCase(identity.name())
                            || existing.identity().uuid().equals(identity.uuid()))
                    .toList();
            if (!conflicts.isEmpty()) {
                if (identityPolicy == IdentityPolicy.REJECT) {
                    return false;
                }
                for (var existing : conflicts) {
                    connections.remove(existing);
                    existing.channel().close();
                }
            }
            connections.add(channel);
            return true;
        }
    }

    private static boolean isLoopback(String hostname) {
        try {
            return InetAddress.getByName(hostname).isLoopbackAddress();
        } catch (UnknownHostException e) {
            return false;
        }
    }

    String prefix() {
        var identity = serverIdentity;
        return identity != null ? "[server: " + identity.name() + "] " : "";
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

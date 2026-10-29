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

import de.dataflair.netty.NettyChannelInitializer;
import de.dataflair.netty.NettyChannelUtils;
import de.dataflair.netty.NettyClientChannel;
import de.dataflair.netty.NettyComponent;
import de.dataflair.netty.NettyIdentities;
import de.dataflair.netty.common.codec.PacketSizeEstimator;
import de.dataflair.netty.filter.AsyncConnectionFilter;
import de.dataflair.netty.filter.ConnectionFilter;
import de.dataflair.netty.filter.Filter;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.ScheduledFuture;
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
 *     <li>Connections are limited globally ({@link #maxConnections}), before authentication
 *     ({@link #maxUnauthenticatedConnections}) and per prefix ({@link #maxConnectionsPerAddress}); new connections
 *     ({@link #maxNewConnectionsPerPrefix}) and authentication attempts ({@link #maxAuthFailuresPerMinute}) are
 *     rate limited per prefix (IPv4 /32, IPv6 {@link #ipv6PrefixLength}).</li>
 *     <li>The UUID of a client is derived from its canonical name ({@link NettyIdentities}) unless
 *     {@link #trustClientUuid} is set. A second connection with the canonical name or UUID of an authenticated one
 *     is handled by the {@link #identityPolicy} (default {@link IdentityPolicy#REJECT}).</li>
 *     <li>Authenticated connections are closed after {@link #idleTimeoutMillis} without receiving anything (the
 *     client sends keep-alives) and after {@code maxRejectedPackets} rejected packets.</li>
 * </ul>
 * Filters, listeners, responders and actions run on the event loop of the connection and must not block.
 */
@Getter
public final class NettyServer extends NettyComponent {
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
    public static final int DEFAULT_MAX_UNAUTHENTICATED_CONNECTIONS = 512;
    public static final int DEFAULT_MAX_NEW_CONNECTIONS_PER_PREFIX = 30;
    public static final long DEFAULT_NEW_CONNECTION_WINDOW_MILLIS = 10_000;
    /**
     * Interval in which the per-prefix limiters drop prefixes whose budget has refilled.
     */
    public static final long LIMITER_CLEANUP_INTERVAL_MILLIS = 10_000;
    public static final int DEFAULT_BACKLOG = 1024;

    private static final Logger LOGGER = LoggerFactory.getLogger(NettyServer.class);

    private final EventLoopGroup workerGroup = NettyChannelUtils.createEventLoopGroup(0);
    private final List<Filter<?>> filters = new CopyOnWriteArrayList<>();
    private final List<AsyncConnectionFilter> asyncConnectionFilters = new CopyOnWriteArrayList<>();
    private final NettyServerPacketTransmitter packetTransmitter;
    /**
     * Authenticated connections. Thread-safe; entries are added after a successful authentication and removed
     * when the channel becomes inactive.
     */
    private final List<NettyClientChannel> connections = new CopyOnWriteArrayList<>();
    @Getter(AccessLevel.NONE)
    private final Object identityLock = new Object();
    @Setter
    private volatile NettyClientChannel.Identity serverIdentity;
    /**
     * Time in milliseconds a connection may stay unauthenticated before it is closed; 0 or less disables the
     * timeout (logged as a warning). Must be set before {@link #initialize()}.
     */
    @Setter
    private volatile long authTimeoutMillis = DEFAULT_AUTH_TIMEOUT_MILLIS;
    /**
     * Time in milliseconds an authenticated connection may stay without the server receiving anything before it is
     * closed (reader idle; the client sends a {@code KeepAlivePacket} when it has nothing else to send); 0 disables
     * the timeout (logged as a warning). Must be set before {@link #initialize()}.
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
     * Maximum number of open connections that have not authenticated yet, default
     * {@value #DEFAULT_MAX_UNAUTHENTICATED_CONNECTIONS}. Must be set before {@link #initialize()}.
     */
    @Setter
    private volatile int maxUnauthenticatedConnections = DEFAULT_MAX_UNAUTHENTICATED_CONNECTIONS;
    /**
     * New connections a prefix may open per {@link #newConnectionWindowMillis}, default
     * {@value #DEFAULT_MAX_NEW_CONNECTIONS_PER_PREFIX}; 0 or less disables the limit. Must be set before
     * {@link #initialize()}.
     */
    @Setter
    private volatile int maxNewConnectionsPerPrefix = DEFAULT_MAX_NEW_CONNECTIONS_PER_PREFIX;
    /**
     * Window of {@link #maxNewConnectionsPerPrefix} in milliseconds, default 10 s.
     */
    @Setter
    private volatile long newConnectionWindowMillis = DEFAULT_NEW_CONNECTION_WINDOW_MILLIS;
    /**
     * Bits of an IPv6 address that form the prefix the per-address limits are counted for, default 64. IPv4
     * addresses are counted per address. Must be set before {@link #initialize()}.
     */
    @Setter
    private volatile int ipv6PrefixLength = PrefixRateLimiter.DEFAULT_IPV6_PREFIX_LENGTH;
    /**
     * Maximum number of prefixes each rate limiter tracks; the least recently used one is dropped beyond that.
     */
    @Setter
    private volatile int maxTrackedPrefixes = PrefixRateLimiter.DEFAULT_MAX_TRACKED_PREFIXES;
    /**
     * If true the server uses the UUID the client sends instead of deriving it from the canonical name. Off by
     * default. With it on, a {@link ConnectionFilter} must check that the client may use that UUID, otherwise a
     * client can take over or block the UUID of another one.
     */
    @Setter
    private volatile boolean trustClientUuid;
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
    @Getter(AccessLevel.NONE)
    private volatile ScheduledFuture<?> limiterCleanup;
    private volatile Channel serverChannel;

    public NettyServer(@NotNull String hostname, int port) {
        super(1, hostname, port);
        this.packetTransmitter = new NettyServerPacketTransmitter(bossGroup(), packet -> {

        });
    }

    public <T extends Filter<?>> NettyServer addFilter(@NotNull T filter) {
        this.filters.add(filter);
        return this;
    }

    /**
     * Adds a filter that decides asynchronously. It runs after all synchronous {@link ConnectionFilter}s.
     */
    public NettyServer addFilter(@NotNull AsyncConnectionFilter filter) {
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

        var prefixLength = ipv6PrefixLength;
        var tracked = maxTrackedPrefixes;
        var limiter = new AuthFailureLimiter(maxAuthFailuresPerMinute, prefixLength, tracked);
        var connectionRate = new PrefixRateLimiter(maxNewConnectionsPerPrefix, newConnectionWindowMillis,
                prefixLength, tracked);
        this.authFailureLimiter = limiter;
        this.connectionLimiter = new ConnectionLimiter(maxConnections, maxConnectionsPerAddress,
                maxUnauthenticatedConnections, limiter, connectionRate, prefixLength);
        this.limiterCleanup = bossGroup().scheduleAtFixedRate(() -> {
            limiter.cleanup();
            connectionRate.cleanup();
        }, LIMITER_CLEANUP_INTERVAL_MILLIS, LIMITER_CLEANUP_INTERVAL_MILLIS, TimeUnit.MILLISECONDS);
        var handshakeTimeout = authTimeout > 0 ? Math.min(handshakeTimeoutMillis(), authTimeout)
                : handshakeTimeoutMillis();

        var initializer = new NettyChannelInitializer(this.serverIdentity, maxFrameLength(), preAuthMaxFrameLength(),
                packetRegistry(), true) {
            @Override
            public SimpleChannelInboundHandler<?> handler() {
                return new NettyServerHandler(NettyServer.this);
            }
        };
        initializer.firstHandler(connectionLimiter).maxOutboundBufferBytes(maxOutboundBufferBytes());
        if (ssl != null) {
            initializer.sslHandlerFactory(channel -> {
                var handler = ssl.newHandler(channel.alloc());
                handler.setHandshakeTimeoutMillis(handshakeTimeout);
                // failed or timed out handshakes count like failed authentications
                handler.handshakeFuture().addListener(future -> {
                    if (!future.isSuccess() && ConnectionLimiter.isAdmitted(channel)
                            && channel.remoteAddress() instanceof InetSocketAddress remote) {
                        limiter.recordFailure(remote.getAddress());
                    }
                });
                return handler;
            });
        }
        new ServerBootstrap()
                .group(bossGroup(), workerGroup)
                .channelFactory(NettyChannelUtils.buildChannelFactory())
                .option(ChannelOption.SO_BACKLOG, backlog)
                .childHandler(initializer)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childOption(ChannelOption.IP_TOS, 24)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childOption(ChannelOption.WRITE_BUFFER_WATER_MARK, writeBufferWaterMark())
                .childOption(ChannelOption.MESSAGE_SIZE_ESTIMATOR, PacketSizeEstimator.INSTANCE)
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
     * @return the identity the server uses for a client that sent {@code sent}: the name as sent and, unless
     * {@link #trustClientUuid} is set, the UUID derived from the canonical name
     */
    public @NotNull NettyClientChannel.Identity effectiveIdentity(@NotNull NettyClientChannel.Identity sent) {
        if (trustClientUuid && sent.uuid() != null) {
            return new NettyClientChannel.Identity(sent.name(), sent.uuid());
        }
        return new NettyClientChannel.Identity(sent.name(), NettyIdentities.derivedUuid(sent.name()));
    }

    /**
     * Adds an authenticated connection, applying the {@link #identityPolicy}. Conflicts are detected by canonical
     * name and by the effective UUID only, both fixed by the server.
     *
     * @return false if the connection was rejected because its identity is in use
     */
    boolean register(@NotNull NettyClientChannel channel) {
        var identity = channel.identity();
        var canonical = NettyIdentities.canonicalName(identity.name());
        synchronized (identityLock) {
            var conflicts = connections.stream()
                    .filter(existing -> NettyIdentities.canonicalName(existing.identity().name()).equals(canonical)
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
        var cleanup = this.limiterCleanup;
        if (cleanup != null) {
            cleanup.cancel(false);
        }
        var channel = this.serverChannel;
        if (channel != null) {
            channel.close();
        }
        this.workerGroup.shutdownGracefully(quietPeriod, timeout, unit);
        return super.shutdownGracefully(quietPeriod, timeout, unit);
    }

    /**
     * @return the authenticated channels whose name has the same canonical form as {@code name}
     */
    public List<NettyClientChannel> clientChannel(@NotNull String name) {
        var canonical = NettyIdentities.canonicalName(name);
        return this.connections.stream()
                .filter(clientChannel -> NettyIdentities.canonicalName(clientChannel.identity().name())
                        .equals(canonical))
                .toList();
    }

    public NettyClientChannel firstChannel(@NotNull String name) {
        return this.clientChannel(name).stream().findFirst().orElse(null);
    }

    /**
     * @param uuid the effective UUID, by default {@link NettyIdentities#derivedUuid(String)} of the client name
     */
    public NettyClientChannel clientChannel(@NotNull UUID uuid) {
        return this.connections.stream()
                .filter(clientChannel -> uuid.equals(clientChannel.identity().uuid()))
                .findFirst()
                .orElse(null);
    }

}

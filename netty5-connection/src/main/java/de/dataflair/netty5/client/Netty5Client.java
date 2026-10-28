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

import de.dataflair.netty5.Netty5ChannelInitializer;
import de.dataflair.netty5.Netty5ChannelUtils;
import de.dataflair.netty5.Netty5ClientChannel;
import de.dataflair.netty5.Netty5Component;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelOption;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.epoll.Epoll;
import io.netty.util.concurrent.Future;
import lombok.Getter;
import lombok.Setter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Client that authenticates with an {@link de.dataflair.netty5.common.packet.auth.AuthPacket} and reconnects
 * automatically, both after a failed connect and after a lost connection.
 * <p>
 * The {@link #packetTransmitter()} lives as long as the client, so listeners and responders registered on it
 * survive reconnects. {@link #connectionFuture()} is replaced by a new, pending future when the connection is lost
 * and completes when the next connection is established. After {@link #shutdownGracefully(long, long, TimeUnit)}
 * it is no longer replaced, so it keeps the state of the last connection.
 * <p>
 * With an {@link #sslContext()} the {@code AuthPacket} is only sent after the TLS handshake has succeeded, and the
 * server certificate must match the host name ({@code endpointIdentificationAlgorithm=HTTPS}).
 */
@Getter
public final class Netty5Client extends Netty5Component {
    private static final AtomicInteger RECONNECT_THREAD_ID = new AtomicInteger();
    private static final Logger LOGGER = LoggerFactory.getLogger(Netty5Client.class);

    private final Netty5ClientChannel.Identity identity;
    private final Map<String, String> authProperty;
    private final ScheduledExecutorService reconnectScheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
        var thread = new Thread(runnable, "netty5-client-reconnect-" + RECONNECT_THREAD_ID.incrementAndGet());
        thread.setDaemon(true);
        return thread;
    });
    private final Netty5ClientPacketTransmitter packetTransmitter;
    /**
     * Delay between reconnect attempts in milliseconds.
     */
    @Setter
    private volatile long reconnectDelayMillis = 5_000;
    @Setter
    private volatile Netty5ClientChannel thisChannel;
    private volatile Bootstrap bootstrap;
    private volatile boolean shuttingDown;

    public Netty5Client(@NotNull String hostname,
                        int port,
                        @NotNull Netty5ClientChannel.Identity identity,
                        @Nullable Map<String, String> authProperty) {
        super(0, hostname, port);
        this.identity = identity;
        this.authProperty = authProperty == null ? new HashMap<>() : authProperty;
        this.packetTransmitter = new Netty5ClientPacketTransmitter(bossGroup(), packet -> {
            var channel = this.thisChannel;
            if (channel == null) {
                throw new IllegalStateException("Client " + identity.name() + " is not connected");
            }
            channel.sendPacket(packet);
        });
    }

    /**
     * @throws IllegalArgumentException if the configured {@link #sslContext()} is not a client context
     */
    @Override
    public void initialize() throws Exception {
        var initializer = new Netty5ChannelInitializer(this.identity, maxFrameLength(), packetRegistry(), false) {
            @Override
            public SimpleChannelInboundHandler<?> handler() {
                return new Netty5ClientHandler(Netty5Client.this);
            }
        };
        initializer.maxOutboundBufferBytes(maxOutboundBufferBytes());
        var ssl = sslContext();
        if (ssl != null) {
            if (!ssl.isClient()) {
                throw new IllegalArgumentException("The client needs a client SslContext");
            }
            var handshakeTimeout = handshakeTimeoutMillis();
            initializer.sslHandlerFactory(channel -> {
                var handler = ssl.newHandler(channel.alloc(), hostname(), port());
                var engine = handler.engine();
                var parameters = engine.getSSLParameters();
                // verify that the certificate belongs to the host we connect to
                parameters.setEndpointIdentificationAlgorithm("HTTPS");
                engine.setSSLParameters(parameters);
                handler.setHandshakeTimeoutMillis(handshakeTimeout);
                return handler;
            });
        }
        bootstrap = new Bootstrap()
                .group(bossGroup())
                .channelFactory(Netty5ChannelUtils.createChannelFactory())
                .handler(initializer)
                .option(ChannelOption.AUTO_READ, true)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.IP_TOS, 24)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 1000)
                .option(ChannelOption.WRITE_BUFFER_WATER_MARK, writeBufferWaterMark());

        if (Epoll.isAvailable() && Epoll.isTcpFastOpenClientSideAvailable()) {
            bootstrap.option(ChannelOption.TCP_FASTOPEN_CONNECT, true);
        }

        connect();
    }

    private void connect() {
        if (shuttingDown || !isAlive()) {
            return;
        }
        try {
            bootstrap.connect(hostname(), port()).addListener((ChannelFutureListener) future -> {
                if (future.isSuccess()) {
                    return;
                }
                connectionState(ConnectionState.DISCONNECTED);
                scheduleReconnect();
            });
        } catch (RejectedExecutionException e) {
            // event loop group is shutting down
        }
    }

    /**
     * Called by the handler when an established connection is lost.
     */
    void connectionLost() {
        connectionState(ConnectionState.DISCONNECTED);
        // after shutdown no reconnect follows, so a new future would never complete
        if (!shuttingDown && connectionFuture().isDone()) {
            connectionFuture(new CommunicationFuture<>());
        }
        scheduleReconnect();
    }

    private void scheduleReconnect() {
        if (shuttingDown || !isAlive()) {
            return;
        }
        try {
            reconnectScheduler.schedule(() -> {
                if (this.connectionState() == ConnectionState.DISCONNECTED ||
                        this.connectionState() == ConnectionState.SEASON_CLOSED) {
                    LOGGER.info("[client: {}] Attempting to reconnect", identity.name());
                    connect();
                }
            }, reconnectDelayMillis, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException e) {
            // scheduler already shut down
        }
    }

    /**
     * Stops reconnecting, closes the connection and shuts down the event loop group and the reconnect scheduler.
     */
    @Override
    public Future<?> shutdownGracefully(long quietPeriod, long timeout, @NotNull TimeUnit unit) {
        shuttingDown = true;
        reconnectScheduler.shutdownNow();
        var channel = this.thisChannel;
        if (channel != null) {
            channel.channel().close();
        }
        return super.shutdownGracefully(quietPeriod, timeout, unit);
    }
}

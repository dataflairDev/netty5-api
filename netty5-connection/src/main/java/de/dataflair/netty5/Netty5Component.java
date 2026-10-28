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

import de.dataflair.netty5.actions.Action;
import de.dataflair.netty5.common.codec.PacketRegistry;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.handler.ssl.SslContext;
import de.dataflair.netty5.common.log.SafeLog;
import io.netty.util.concurrent.Future;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

@Getter
public abstract class Netty5Component {
    /**
     * Default TLS handshake timeout in milliseconds.
     */
    public static final long DEFAULT_HANDSHAKE_TIMEOUT_MILLIS = 10_000;
    /**
     * Default write buffer water marks: low 32 KiB, high 64 KiB.
     */
    public static final WriteBufferWaterMark DEFAULT_WRITE_BUFFER_WATER_MARK =
            new WriteBufferWaterMark(32 * 1024, 64 * 1024);
    /**
     * Default hard limit of the outbound buffer per channel (8 MiB).
     */
    public static final long DEFAULT_MAX_OUTBOUND_BUFFER_BYTES = 8L * 1024 * 1024;
    private static final Logger LOGGER = LoggerFactory.getLogger(Netty5Component.class);

    @Setter(AccessLevel.PROTECTED)
    private volatile CommunicationFuture<Void> connectionFuture = new CommunicationFuture<>();
    @Getter
    private final EventLoopGroup bossGroup;
    @Getter(AccessLevel.PROTECTED)
    private final String hostname;
    @Getter(AccessLevel.PROTECTED)
    private final int port;
    @Getter
    private final List<Action<?>> actions = new CopyOnWriteArrayList<>();
    @Setter
    private volatile ConnectionState connectionState = ConnectionState.UNDEFINED;
    /**
     * Maximum accepted frame length in bytes, default 16 MiB. Must be set before {@link #initialize()}.
     */
    @Setter
    private volatile int maxFrameLength = Netty5ChannelUtils.DEFAULT_MAX_FRAME_LENGTH;
    /**
     * Maximum accepted frame length in bytes while a channel that requires authentication is not yet
     * authenticated, default 64 KiB. Only used by the server. Must be set before {@link #initialize()}.
     */
    @Setter
    private volatile int preAuthMaxFrameLength = Netty5ChannelUtils.DEFAULT_PRE_AUTH_MAX_FRAME_LENGTH;
    /**
     * Registry used to resolve incoming packet classes. Must be set before {@link #initialize()}.
     */
    @Setter
    private volatile @NotNull PacketRegistry packetRegistry = new PacketRegistry();
    /**
     * TLS context, null for plain TCP (default). A server needs a server context, a client a client context.
     * The {@code SslHandler} is the first handler that sees the bytes of a connection. Must be set before
     * {@link #initialize()}.
     */
    @Setter
    private volatile @Nullable SslContext sslContext;
    /**
     * Maximum time in milliseconds for the TLS handshake, default 10 s. The server uses at most its
     * authentication timeout. Must be set before {@link #initialize()}.
     */
    @Setter
    private volatile long handshakeTimeoutMillis = DEFAULT_HANDSHAKE_TIMEOUT_MILLIS;
    /**
     * Write buffer water marks in bytes (default 32 KiB / 64 KiB). Above the high water mark a channel is
     * unwritable and stops reading until it drops below the low water mark again. Must be set before
     * {@link #initialize()}.
     */
    @Setter
    private volatile @NotNull WriteBufferWaterMark writeBufferWaterMark = DEFAULT_WRITE_BUFFER_WATER_MARK;
    /**
     * Hard limit in bytes for the outbound buffer of one channel, default 8 MiB. A channel whose pending writes
     * exceed it is closed. Must be set before {@link #initialize()}.
     */
    @Setter
    private volatile long maxOutboundBufferBytes = DEFAULT_MAX_OUTBOUND_BUFFER_BYTES;

    public Netty5Component(int bossGroupThreads, @NotNull String hostname, int port) {
        this.bossGroup = Netty5ChannelUtils.createEventLoopGroup(bossGroupThreads);
        this.hostname = hostname;
        this.port = port;
    }

    public boolean isAlive() {
        return !bossGroup.isShutdown() && !bossGroup.isTerminated() && !bossGroup.isShuttingDown();
    }

    public void shutdownGracefully() {
        shutdownGracefully(2, 15, TimeUnit.SECONDS);
    }

    /**
     * Shuts the component down.
     *
     * @return the termination future of the boss group
     */
    public Future<?> shutdownGracefully(long quietPeriod, long timeout, @NotNull TimeUnit unit) {
        return bossGroup.shutdownGracefully(quietPeriod, timeout, unit);
    }

    public ChannelFutureListener deployFuture() {
        return it -> {
            if (it.isSuccess()) {
                connectionFuture.complete(null);
            } else {
                connectionFuture.completeExceptionally(it.cause());
            }
        };
    }

    public <T extends Action<?>> Netty5Component addAction(@NotNull T action) {
        actions.add(action);
        return this;
    }

    public abstract void initialize() throws Exception;

    public enum ConnectionState {
        UNDEFINED,
        CONNECTED,
        DISCONNECTED,
        SEASON_CLOSED
    }

    public static final class CommunicationFuture<E> extends CompletableFuture<E> {
        public CommunicationFuture() {
            this.exceptionally(throwable -> {
                LOGGER.warn("Communication future failed: {}", SafeLog.clean(throwable));
                return null;
            });
        }

        public E sync(E defaultValue, long secondTimeout) {
            try {
                return get(secondTimeout, TimeUnit.SECONDS);
            } catch (Exception e) {
                return defaultValue;
            }
        }

        public E sync(E defaultValue) {
            return sync(defaultValue, 5);
        }
    }

}

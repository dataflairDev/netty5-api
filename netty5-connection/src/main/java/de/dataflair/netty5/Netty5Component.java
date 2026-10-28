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
import io.netty.util.concurrent.Future;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

@Getter
public abstract class Netty5Component {
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
     * Registry used to resolve incoming packet classes. Must be set before {@link #initialize()}.
     */
    @Setter
    private volatile @NotNull PacketRegistry packetRegistry = new PacketRegistry();

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
                throwable.printStackTrace();
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

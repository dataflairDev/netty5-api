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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFactory;
import io.netty.channel.IoHandlerFactory;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.ReflectiveChannelFactory;
import io.netty.channel.ServerChannel;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollIoHandler;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.AttributeKey;
import lombok.experimental.UtilityClass;
import org.jetbrains.annotations.NotNull;

import java.util.UUID;

@UtilityClass
public class Netty5ChannelUtils {
    public final Gson JSON = new GsonBuilder().disableHtmlEscaping().create();
    public final UUID SYSTEM_UUID = UUID.fromString("0f0f0f0f-0f0f-0f0f-f0f0-0f0f0f0f0f0f");

    /**
     * Default maximum frame length (16 MiB).
     */
    public final int DEFAULT_MAX_FRAME_LENGTH = 16 * 1024 * 1024;

    /**
     * Default maximum frame length before authentication (64 KiB). Only applies to channels that require
     * authentication.
     */
    public final int DEFAULT_PRE_AUTH_MAX_FRAME_LENGTH = 64 * 1024;

    /**
     * Set on a server side child channel once the peer has been authenticated.
     * The {@link de.dataflair.netty5.common.codec.PacketDecoder} only accepts the auth packet until it is set.
     */
    public final AttributeKey<Boolean> AUTHENTICATED = AttributeKey.valueOf("netty5-api.authenticated");

    /**
     * Set on a server side child channel while an {@link de.dataflair.netty5.filter.AsyncConnectionFilter} is
     * evaluating its auth packet. No further frame is decoded until it is cleared.
     */
    public final AttributeKey<Boolean> AUTH_PENDING = AttributeKey.valueOf("netty5-api.auth-pending");

    /**
     * Reason for {@link #suspendRead(Channel, int)}: the outbound buffer is above the high water mark.
     */
    public final int READ_SUSPENDED_BACKPRESSURE = 1;
    /**
     * Reason for {@link #suspendRead(Channel, int)}: an asynchronous connection filter is running.
     */
    public final int READ_SUSPENDED_AUTHENTICATION = 2;

    private final AttributeKey<Integer> READ_SUSPENSION = AttributeKey.valueOf("netty5-api.read-suspension");

    /**
     * Stops reading from the channel ({@code autoRead=false}) for the given reason. Reading resumes when every
     * reason has been cleared with {@link #resumeRead(Channel, int)}. Must be called on the channel's event loop.
     */
    public static void suspendRead(@NotNull Channel channel, int reason) {
        var attribute = channel.attr(READ_SUSPENSION);
        var current = attribute.get();
        attribute.set((current == null ? 0 : current) | reason);
        channel.config().setAutoRead(false);
    }

    /**
     * Clears a reason set with {@link #suspendRead(Channel, int)}; turns {@code autoRead} back on if no reason
     * is left. Must be called on the channel's event loop.
     */
    public static void resumeRead(@NotNull Channel channel, int reason) {
        var attribute = channel.attr(READ_SUSPENSION);
        var current = attribute.get();
        var remaining = (current == null ? 0 : current) & ~reason;
        attribute.set(remaining);
        if (remaining == 0) {
            channel.config().setAutoRead(true);
        }
    }

    public static boolean isAuthenticated(@NotNull Channel channel) {
        return Boolean.TRUE.equals(channel.attr(AUTHENTICATED).get());
    }

    public static boolean isAuthPending(@NotNull Channel channel) {
        return Boolean.TRUE.equals(channel.attr(AUTH_PENDING).get());
    }

    public static @NotNull MultiThreadIoEventLoopGroup createEventLoopGroup(int threads) {
        return new MultiThreadIoEventLoopGroup(threads, ioHandlerFactory());
    }

    public static @NotNull IoHandlerFactory ioHandlerFactory() {
        return Epoll.isAvailable() ? EpollIoHandler.newFactory() : NioIoHandler.newFactory();
    }

    public static @NotNull ChannelFactory<? extends SocketChannel> createChannelFactory() {
        return Epoll.isAvailable()
                ? new ReflectiveChannelFactory<>(EpollSocketChannel.class)
                : new ReflectiveChannelFactory<>(NioSocketChannel.class);
    }

    public static @NotNull ChannelFactory<? extends ServerChannel> buildChannelFactory() {
        return Epoll.isAvailable()
                ? new ReflectiveChannelFactory<>(EpollServerSocketChannel.class)
                : new ReflectiveChannelFactory<>(NioServerSocketChannel.class);
    }

}

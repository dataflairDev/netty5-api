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

import de.dataflair.netty.client.NettyClientPacketTransmitter;
import de.dataflair.netty.common.codec.CodecBuffer;
import de.dataflair.netty.common.packet.Packet;
import io.netty.channel.Channel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.jetbrains.annotations.NotNull;

import java.util.Map;
import java.util.UUID;

@Getter
@AllArgsConstructor
public final class NettyClientChannel {
    @Setter
    private Identity identity;
    private final Channel channel;
    @Setter
    private NettyClientPacketTransmitter transmitter;

    public void sendPacket(@NotNull Packet packet) {
        channel.writeAndFlush(packet);
    }

    /**
     * @return true if the channel is open and its outbound buffer is below the high water mark
     * ({@code writeBufferWaterMark}); a sender that produces a lot should wait while this is false
     */
    public boolean isWritable() {
        return channel.isActive() && channel.isWritable();
    }

    @Getter
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Identity implements CodecBuffer.WriteReadStream {
        private String name;
        private UUID uuid;

        @Override
        public void writeBuffer(@NotNull CodecBuffer codecBuffer) {
            codecBuffer.writeString(name)
                    .writeUniqueId(uuid);
        }

        @Override
        public void readBuffer(@NotNull CodecBuffer codecBuffer) {
            this.name = codecBuffer.readString();
            this.uuid = codecBuffer.readUniqueId();
        }
    }

    /**
     * What a {@link de.dataflair.netty.filter.ConnectionFilter} sees. {@code clientChannel().identity()} holds the
     * name as sent and the UUID the server will use (derived from the canonical name unless the server trusts
     * client UUIDs).
     */
    public record AuthType(NettyClientChannel clientChannel, Map<String, String> authProperty) {
        /**
         * @return the canonical name ({@link NettyIdentities#canonicalName(String)}); bind tokens to this
         */
        public String canonicalName() {
            return NettyIdentities.canonicalName(clientChannel.identity().name());
        }
    }

}

package de.dataflair.netty.common.packet.auth;



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

import de.dataflair.netty.NettyClientChannel;
import de.dataflair.netty.common.codec.CodecBuffer;
import de.dataflair.netty.common.packet.Packet;
import lombok.Getter;
import org.jetbrains.annotations.NotNull;

import java.util.HashMap;
import java.util.Map;

@Getter
public class AuthPacket extends Packet {
    /**
     * Maximum number of auth properties. The auth packet is read before the peer is authenticated, so its size
     * is bounded.
     */
    public static final int MAX_PROPERTIES = 256;

    protected final NettyClientChannel.Identity identity;
    protected final Map<String, String> properties;

    public AuthPacket(@NotNull NettyClientChannel.Identity identity, @NotNull Map<String, String> properties) {
        if (properties.size() > MAX_PROPERTIES) {
            throw new IllegalArgumentException("Too many auth properties: " + properties.size()
                    + " (max " + MAX_PROPERTIES + ")");
        }
        this.identity = identity;
        this.properties = properties;
        buffer.writeStream(this.identity)
                .writeMap(this.properties, CodecBuffer::writeString, CodecBuffer::writeString);
    }

    public AuthPacket(@NotNull CodecBuffer buffer) {
        super(buffer);
        this.identity = buffer.readStream(new NettyClientChannel.Identity());
        this.properties = buffer.readMap(new HashMap<>(), buffer::readString, buffer::readString, MAX_PROPERTIES);
    }
}

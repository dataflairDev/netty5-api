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

import de.dataflair.netty5.Netty5ClientChannel;
import de.dataflair.netty5.common.packet.Netty5PacketTransmitter;
import de.dataflair.netty5.common.packet.Packet;
import io.netty.channel.EventLoopGroup;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.function.Consumer;

public final class Netty5ClientPacketTransmitter extends Netty5PacketTransmitter {
    public Netty5ClientPacketTransmitter(@NotNull EventLoopGroup eventExecutors,
                                         @NotNull Consumer<Packet> packetConsumer) {
        super(eventExecutors, packetConsumer);
    }

    @Override
    public void callActions(@NotNull Packet packet, @Nullable Netty5ClientChannel sender) {
    }
}

package de.dataflair.netty5.common.packet;



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

import de.dataflair.netty5.common.codec.CodecBuffer;
import lombok.Getter;
import lombok.Setter;
import org.jetbrains.annotations.NotNull;

import java.util.UUID;

/**
 * Packet that is matched to its counterpart by a query id. The query id is read by the constructor; the payload is
 * read by {@link #readBuffer(CodecBuffer)}, which is called by {@link PacketFactory} after construction and not
 * from the constructor. Subclass field initialisers therefore never overwrite decoded values.
 */
@Setter
@Getter
public abstract class RequestPacket extends Packet implements CodecBuffer.WriteReadStream {
    protected UUID queryId;

    public RequestPacket() {
        super();
    }

    public RequestPacket(@NotNull CodecBuffer buffer) {
        super(buffer);
        this.queryId = buffer.readUniqueId();
    }

    @Override
    protected void readContent(@NotNull CodecBuffer buffer) {
        this.readBuffer(buffer);
    }
}

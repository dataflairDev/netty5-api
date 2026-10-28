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
import org.jetbrains.annotations.NotNull;

/**
 * @deprecated use {@link RequestPacket}. Shares the query id field and wire format of {@link RequestPacket}.
 */
@Deprecated
public abstract class QueryPacket extends RequestPacket {

    public QueryPacket() {
        super();
    }

    public QueryPacket(@NotNull CodecBuffer buffer) {
        super(buffer);
    }
}

package de.dataflair.netty5.common.packet.json;

import de.dataflair.netty5.common.codec.CodecBuffer;
import de.dataflair.netty5.common.packet.Packet;
import lombok.Getter;
import org.jetbrains.annotations.NotNull;

@Getter
public class JsonPacket extends Packet {

    private final String json;

    public JsonPacket(String json) {
        this.json = json;
        this.buffer.writeNullable(this.json, codecBuffer -> codecBuffer.writeString(json));
    }

    public JsonPacket(@NotNull CodecBuffer buffer) {
        super(buffer);
        this.json = buffer.readNullable(String.class, buffer::readString);
    }
}

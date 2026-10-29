package de.dataflair.netty5.common.packet.json;

import de.dataflair.netty5.common.codec.CodecBuffer;
import de.dataflair.netty5.common.packet.RespondPacket;
import lombok.Getter;
import org.jetbrains.annotations.NotNull;

@Getter
public class JsonRespondPacket extends RespondPacket {

    private String json;

    public JsonRespondPacket(String json) {
        this.json = json;
    }

    public JsonRespondPacket(@NotNull CodecBuffer buffer) {
        super(buffer);
    }

    @Override
    public void writeBuffer(@NotNull CodecBuffer codecBuffer) {
        codecBuffer.writeNullable(this.json, _ -> codecBuffer.writeString(json));
    }

    @Override
    public void readBuffer(@NotNull CodecBuffer codecBuffer) {
        this.json = codecBuffer.readNullable(String.class, codecBuffer::readString);
    }
}

package de.dataflair.netty5.testpackets;

import de.dataflair.netty5.common.codec.CodecBuffer;
import de.dataflair.netty5.common.packet.RespondPacket;

public class TestRespondPacket extends RespondPacket {
    private String value = "initializer";

    public TestRespondPacket(String value) {
        this.value = value;
    }

    public TestRespondPacket(CodecBuffer buffer) {
        super(buffer);
    }

    @Override
    public void writeBuffer(CodecBuffer codecBuffer) {
        codecBuffer.writeString(value);
    }

    @Override
    public void readBuffer(CodecBuffer codecBuffer) {
        this.value = codecBuffer.readString();
    }

    public String value() {
        return value;
    }
}

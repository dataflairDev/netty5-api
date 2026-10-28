package de.dataflair.netty5.testpackets;

import de.dataflair.netty5.common.codec.CodecBuffer;
import de.dataflair.netty5.common.packet.QueryPacket;

@SuppressWarnings("deprecation")
public class TestQueryPacket extends QueryPacket {
    private String text = "initializer";

    public TestQueryPacket(String text) {
        this.text = text;
    }

    public TestQueryPacket(CodecBuffer buffer) {
        super(buffer);
    }

    @Override
    public void writeBuffer(CodecBuffer codecBuffer) {
        codecBuffer.writeString(text);
    }

    @Override
    public void readBuffer(CodecBuffer codecBuffer) {
        this.text = codecBuffer.readString();
    }

    public String text() {
        return text;
    }
}

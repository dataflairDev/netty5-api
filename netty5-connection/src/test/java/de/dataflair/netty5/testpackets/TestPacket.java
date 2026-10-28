package de.dataflair.netty5.testpackets;

import de.dataflair.netty5.common.codec.CodecBuffer;
import de.dataflair.netty5.common.packet.Packet;

public class TestPacket extends Packet {
    private final String message;
    private final int number;

    public TestPacket(String message, int number) {
        this.message = message;
        this.number = number;
        buffer.writeString(message).writeInt(number);
    }

    public TestPacket(CodecBuffer buffer) {
        super(buffer);
        this.message = buffer.readString();
        this.number = buffer.readInt();
    }

    public String message() {
        return message;
    }

    public int number() {
        return number;
    }
}

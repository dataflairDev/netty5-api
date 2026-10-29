package de.dataflair.netty5.testpackets;

import de.dataflair.netty5.common.codec.CodecBuffer;

public class NotAPacket {
    static {
        ProbeFlags.NOT_A_PACKET_INITIALIZED.set(true);
    }

    public NotAPacket(CodecBuffer buffer) {
    }
}

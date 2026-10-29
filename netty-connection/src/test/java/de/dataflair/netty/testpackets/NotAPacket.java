package de.dataflair.netty.testpackets;

import de.dataflair.netty.common.codec.CodecBuffer;

public class NotAPacket {
    static {
        ProbeFlags.NOT_A_PACKET_INITIALIZED.set(true);
    }

    public NotAPacket(CodecBuffer buffer) {
    }
}

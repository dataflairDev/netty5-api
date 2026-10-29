package de.dataflair.netty.testpackets;

import de.dataflair.netty.common.codec.CodecBuffer;
import de.dataflair.netty.common.packet.Packet;

public abstract class AbstractProbePacket extends Packet {
    static {
        ProbeFlags.ABSTRACT_PACKET_INITIALIZED.set(true);
    }

    public AbstractProbePacket(CodecBuffer buffer) {
        super(buffer);
    }
}

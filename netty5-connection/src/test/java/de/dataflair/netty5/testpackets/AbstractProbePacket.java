package de.dataflair.netty5.testpackets;

import de.dataflair.netty5.common.codec.CodecBuffer;
import de.dataflair.netty5.common.packet.Packet;

public abstract class AbstractProbePacket extends Packet {
    static {
        ProbeFlags.ABSTRACT_PACKET_INITIALIZED.set(true);
    }

    public AbstractProbePacket(CodecBuffer buffer) {
        super(buffer);
    }
}

package de.dataflair.netty5.testpackets;

import de.dataflair.netty5.common.codec.CodecBuffer;
import de.dataflair.netty5.common.packet.Packet;

public class RestrictedProbePacket extends Packet {
    static {
        ProbeFlags.RESTRICTED_PACKET_INITIALIZED.set(true);
    }

    public RestrictedProbePacket() {
    }

    public RestrictedProbePacket(CodecBuffer buffer) {
        super(buffer);
    }
}

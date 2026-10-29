package de.dataflair.netty.testpackets;

import de.dataflair.netty.common.codec.CodecBuffer;
import de.dataflair.netty.common.packet.Packet;

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

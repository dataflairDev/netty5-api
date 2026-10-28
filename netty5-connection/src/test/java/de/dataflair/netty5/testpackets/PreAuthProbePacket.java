package de.dataflair.netty5.testpackets;

import de.dataflair.netty5.common.codec.CodecBuffer;
import de.dataflair.netty5.common.packet.Packet;

public class PreAuthProbePacket extends Packet {
    static {
        ProbeFlags.PRE_AUTH_PACKET_INITIALIZED.set(true);
    }

    public PreAuthProbePacket() {
    }

    public PreAuthProbePacket(CodecBuffer buffer) {
        super(buffer);
    }
}

package de.dataflair.netty.testpackets;

import de.dataflair.netty.common.codec.CodecBuffer;
import de.dataflair.netty.common.packet.Packet;

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

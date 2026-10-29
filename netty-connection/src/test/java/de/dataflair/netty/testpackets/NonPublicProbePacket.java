package de.dataflair.netty.testpackets;

import de.dataflair.netty.common.codec.CodecBuffer;
import de.dataflair.netty.common.packet.Packet;

/**
 * Package-private packet; the registry must reject it without initialising it.
 */
class NonPublicProbePacket extends Packet {
    static {
        ProbeFlags.NON_PUBLIC_PACKET_INITIALIZED.set(true);
    }

    public NonPublicProbePacket(CodecBuffer buffer) {
        super(buffer);
    }
}

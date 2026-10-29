package de.dataflair.netty5.testpackets;

import de.dataflair.netty5.common.codec.CodecBuffer;
import de.dataflair.netty5.common.packet.Packet;

/**
 * Its static initialiser throws, so instantiating it raises an {@link ExceptionInInitializerError}.
 */
public class ExplodingInitPacket extends Packet {
    static {
        if (Boolean.TRUE) {
            throw new IllegalStateException("static initialiser fails");
        }
    }

    public ExplodingInitPacket(CodecBuffer buffer) {
        super(buffer);
    }
}

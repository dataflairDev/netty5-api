package de.dataflair.netty.testpackets;

import de.dataflair.netty.common.codec.CodecBuffer;
import de.dataflair.netty.common.packet.Packet;

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

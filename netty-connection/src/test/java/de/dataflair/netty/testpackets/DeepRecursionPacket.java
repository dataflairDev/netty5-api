package de.dataflair.netty5.testpackets;

import de.dataflair.netty5.common.codec.CodecBuffer;
import de.dataflair.netty5.common.packet.Packet;

/**
 * Its decoding constructor recurses until a {@link StackOverflowError} is thrown.
 */
public class DeepRecursionPacket extends Packet {
    public DeepRecursionPacket(CodecBuffer buffer) {
        super(buffer);
        recurse(0);
    }

    private static int recurse(int depth) {
        return recurse(depth + 1) + 1;
    }
}

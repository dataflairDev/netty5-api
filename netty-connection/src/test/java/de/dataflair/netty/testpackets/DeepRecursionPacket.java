package de.dataflair.netty.testpackets;

import de.dataflair.netty.common.codec.CodecBuffer;
import de.dataflair.netty.common.packet.Packet;

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

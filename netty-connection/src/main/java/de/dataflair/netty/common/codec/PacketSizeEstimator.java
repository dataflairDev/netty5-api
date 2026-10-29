package de.dataflair.netty.common.codec;

import de.dataflair.netty.common.packet.Packet;
import io.netty.channel.DefaultMessageSizeEstimator;
import io.netty.channel.MessageSizeEstimator;

/**
 * Estimates a {@link Packet} that has not been encoded yet with the readable bytes of its content plus
 * {@link #OVERHEAD} bytes for the frame header and class name. Netty counts writes that are queued from outside
 * the event loop with this estimate, so a thread that writes many large packets makes the channel unwritable
 * before they are encoded. Other messages are estimated by {@link DefaultMessageSizeEstimator}.
 */
public final class PacketSizeEstimator implements MessageSizeEstimator {
    public static final PacketSizeEstimator INSTANCE = new PacketSizeEstimator();
    /**
     * Bytes added to the content: three length fields and a typical class name.
     */
    public static final int OVERHEAD = 3 * Integer.BYTES + 64;

    private static final Handle HANDLE = new Handle() {
        private final Handle fallback = DefaultMessageSizeEstimator.DEFAULT.newHandle();

        @Override
        public int size(Object msg) {
            if (msg instanceof Packet packet) {
                return size(packet);
            }
            return fallback.size(msg);
        }

        private int size(Packet packet) {
            var buffer = packet.buffer();
            var content = buffer == null || buffer.origin() == null ? 0 : buffer.origin().readableBytes();
            return (int) Math.min(Integer.MAX_VALUE, (long) content + OVERHEAD);
        }
    };

    private PacketSizeEstimator() {
    }

    @Override
    public Handle newHandle() {
        return HANDLE;
    }
}

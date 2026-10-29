package de.dataflair.netty.common.packet;

import de.dataflair.netty.common.codec.CodecBuffer;
import lombok.experimental.UtilityClass;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.InvocationTargetException;

/**
 * Creates packets from a buffer in two phases: the {@code (CodecBuffer)} constructor, then
 * {@link Packet#readContent(CodecBuffer)}. Use this instead of calling the constructor directly when decoding
 * {@link RequestPacket}s or {@link RespondPacket}s by hand.
 */
@UtilityClass
public class PacketFactory {

    public static <P extends Packet> @NotNull P create(@NotNull Class<P> packetClass, @NotNull CodecBuffer buffer)
            throws ReflectiveOperationException {
        final P packet;
        try {
            packet = packetClass.getConstructor(CodecBuffer.class).newInstance(buffer);
        } catch (InvocationTargetException e) {
            if (e.getCause() instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            if (e.getCause() instanceof Error error) {
                // StackOverflowError, LinkageError, ...: the decoder closes the channel
                throw error;
            }
            throw e;
        }
        packet.readContent(buffer);
        return packet;
    }
}

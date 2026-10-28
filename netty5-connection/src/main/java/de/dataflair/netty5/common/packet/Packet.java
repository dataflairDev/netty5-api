package de.dataflair.netty5.common.packet;

import de.dataflair.netty5.common.codec.CodecBuffer;
import lombok.Getter;
import lombok.Setter;
import org.jetbrains.annotations.NotNull;

/**
 * Base class of all packets.
 * <p>
 * A packet needs a public {@code (CodecBuffer)} constructor that reads its fields. Received packets are created by
 * {@link PacketFactory#create(Class, CodecBuffer)}: it calls that constructor and afterwards
 * {@link #readContent(CodecBuffer)}. Packets whose fields are declared in a subclass of an abstract base (such as
 * {@link RequestPacket} and {@link RespondPacket}) read them in {@link #readContent(CodecBuffer)}, after all field
 * initialisers have run.
 * <p>
 * Buffer ownership: the buffer of a packet is an unpooled heap buffer owned by the packet and reclaimed by the
 * garbage collector. The encoder reads it without consuming it, so a packet may be sent several times. No manual
 * {@code release()} is required.
 */
@Getter
@Setter
public abstract class Packet {
    protected CodecBuffer buffer;

    public Packet() {
        this.buffer = CodecBuffer.allocate();
    }

    public Packet(@NotNull CodecBuffer buffer) {
        this.buffer = buffer;
    }

    /**
     * Second decoding phase, called by {@link PacketFactory} after the {@code (CodecBuffer)} constructor.
     * Field initialisers of subclasses have already run at this point, so values read here are not overwritten.
     *
     * @param buffer the buffer the packet was constructed with
     */
    protected void readContent(@NotNull CodecBuffer buffer) {
    }
}

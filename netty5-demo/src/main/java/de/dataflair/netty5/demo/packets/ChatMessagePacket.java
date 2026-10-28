package de.dataflair.netty5.demo.packets;

import de.dataflair.netty5.common.codec.CodecBuffer;
import de.dataflair.netty5.common.packet.Packet;
import org.jetbrains.annotations.NotNull;

/**
 * Plain packet without a response, sent with {@code publishPacket} or {@code sendPacket}.
 * <p>
 * Every packet class needs to be public, concrete and have a public {@code (CodecBuffer)} constructor, otherwise
 * the {@link de.dataflair.netty5.common.codec.PacketRegistry} rejects it.
 */
public class ChatMessagePacket extends Packet {
    private final String sender;
    private final String text;

    /**
     * Sending side: write the fields into the packet's own buffer.
     */
    public ChatMessagePacket(@NotNull String sender, @NotNull String text) {
        this.sender = sender;
        this.text = text;
        buffer.writeString(sender).writeString(text);
    }

    /**
     * Receiving side, called by {@link de.dataflair.netty5.common.packet.PacketFactory}. Read in the same order
     * as written. Reading in the constructor is fine here because the fields are final and have no initialiser;
     * for non-final fields with an initialiser override {@code readContent(CodecBuffer)} instead
     * (see {@link UserLookupResponse}).
     */
    public ChatMessagePacket(@NotNull CodecBuffer buffer) {
        super(buffer);
        this.sender = buffer.readString();
        this.text = buffer.readString();
    }

    public String sender() {
        return sender;
    }

    public String text() {
        return text;
    }
}

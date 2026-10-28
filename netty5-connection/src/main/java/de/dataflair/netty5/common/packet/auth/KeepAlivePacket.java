package de.dataflair.netty5.common.packet.auth;

import de.dataflair.netty5.common.codec.CodecBuffer;
import de.dataflair.netty5.common.packet.Packet;
import org.jetbrains.annotations.NotNull;

/**
 * Empty packet the client sends when it has written nothing for {@code keepAliveIntervalMillis}, so the reader
 * idle timeout of the server does not close a quiet but healthy connection. Always registered in a
 * {@link de.dataflair.netty5.common.codec.PacketRegistry}. The server drops it after authentication before any
 * filter or listener; before authentication it is rejected like every packet except {@link AuthPacket}.
 */
public final class KeepAlivePacket extends Packet {
    public KeepAlivePacket() {
    }

    public KeepAlivePacket(@NotNull CodecBuffer buffer) {
        super(buffer);
    }
}

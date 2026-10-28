package de.dataflair.netty5.common.packet.auth;

import de.dataflair.netty5.Netty5ClientChannel;
import de.dataflair.netty5.common.codec.CodecBuffer;
import de.dataflair.netty5.common.packet.Packet;
import lombok.Getter;
import org.jetbrains.annotations.NotNull;

/**
 * Sent by the server as the first packet after a successful authentication. It carries the identity the server
 * uses for the connection: the name as sent and the effective UUID (derived from the canonical name unless the
 * server trusts client UUIDs). Always registered in a {@link de.dataflair.netty5.common.codec.PacketRegistry};
 * the client consumes it, it never reaches listeners.
 */
@Getter
public final class AuthAcceptedPacket extends Packet {
    private final Netty5ClientChannel.Identity identity;

    public AuthAcceptedPacket(@NotNull Netty5ClientChannel.Identity identity) {
        this.identity = identity;
        buffer.writeStream(identity);
    }

    public AuthAcceptedPacket(@NotNull CodecBuffer buffer) {
        super(buffer);
        this.identity = buffer.readStream(new Netty5ClientChannel.Identity());
    }
}

package de.dataflair.netty.common.packet.auth;

import de.dataflair.netty.NettyClientChannel;
import de.dataflair.netty.common.codec.CodecBuffer;
import de.dataflair.netty.common.packet.Packet;
import lombok.Getter;
import org.jetbrains.annotations.NotNull;

/**
 * Sent by the server as the first packet after a successful authentication. It carries the identity the server
 * uses for the connection: the name as sent and the effective UUID (derived from the canonical name unless the
 * server trusts client UUIDs). Always registered in a {@link de.dataflair.netty.common.codec.PacketRegistry};
 * the client consumes it, it never reaches listeners.
 */
@Getter
public final class AuthAcceptedPacket extends Packet {
    private final NettyClientChannel.Identity identity;

    public AuthAcceptedPacket(@NotNull NettyClientChannel.Identity identity) {
        this.identity = identity;
        buffer.writeStream(identity);
    }

    public AuthAcceptedPacket(@NotNull CodecBuffer buffer) {
        super(buffer);
        this.identity = buffer.readStream(new NettyClientChannel.Identity());
    }
}

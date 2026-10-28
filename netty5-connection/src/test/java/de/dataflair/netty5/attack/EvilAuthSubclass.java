package de.dataflair.netty5.attack;

import de.dataflair.netty5.Netty5ClientChannel;
import de.dataflair.netty5.common.codec.CodecBuffer;
import de.dataflair.netty5.common.packet.auth.AuthPacket;
import org.jetbrains.annotations.NotNull;

import java.util.Map;

/**
 * A real {@code instanceof AuthPacket} subclass. {@code Netty5ServerHandler.channelRead0} authenticates on
 * {@code instanceof AuthPacket}, so if the decoder resolved this class pre-auth it could authenticate under a
 * different class name. The decoder must reject it because its name is not exactly {@code AuthPacket}.
 */
public class EvilAuthSubclass extends AuthPacket {
    public EvilAuthSubclass(@NotNull Netty5ClientChannel.Identity identity, @NotNull Map<String, String> properties) {
        super(identity, properties);
    }

    public EvilAuthSubclass(@NotNull CodecBuffer buffer) {
        super(buffer);
    }
}

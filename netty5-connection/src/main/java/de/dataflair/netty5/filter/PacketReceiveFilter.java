package de.dataflair.netty5.filter;

import de.dataflair.netty5.Netty5ClientChannel;
import de.dataflair.netty5.common.packet.Packet;
import org.jetbrains.annotations.NotNull;

/**
 * Decides per received packet whether it is passed on to responders and listeners on the server.
 * <p>
 * This is the place for authorization: the library does not know which packets a peer may send (there is no
 * direction registry), so a server that gives clients different rights checks {@code senderChannel} and the
 * packet class here. A rejected packet is dropped and counts towards
 * {@code Netty5Server#maxRejectedPackets}. Runs on the event loop and must not block.
 */
public abstract class PacketReceiveFilter extends Filter<PacketReceiveFilter.FilterValue> {
    public record FilterValue(@NotNull Packet recievedPacket, @NotNull Netty5ClientChannel senderChannel) {
    }
}

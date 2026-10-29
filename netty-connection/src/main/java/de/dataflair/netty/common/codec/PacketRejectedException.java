package de.dataflair.netty5.common.codec;

import io.netty.handler.codec.DecoderException;

/**
 * Thrown by the {@link PacketDecoder} when an incoming frame names a class that must not be instantiated
 * (unknown, not a packet, abstract, not registered, or not allowed before authentication).
 */
public class PacketRejectedException extends DecoderException {
    public PacketRejectedException(String message) {
        super(message);
    }

    public PacketRejectedException(String message, Throwable cause) {
        super(message, cause);
    }
}

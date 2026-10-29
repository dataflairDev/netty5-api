package de.dataflair.netty.demo.packets;

import de.dataflair.netty.common.codec.CodecBuffer;
import de.dataflair.netty.common.packet.RequestPacket;
import org.jetbrains.annotations.NotNull;

/**
 * Request sent with {@code queryPacket} or {@code queryPacketDirect}; answered by a responder registered with
 * {@code listenQuery}.
 * <p>
 * Request and respond packets do not write their payload in the constructor. The transmitter writes the query id
 * and then calls {@link #writeBuffer(CodecBuffer)} when the request is sent; on the receiving side
 * {@link #readBuffer(CodecBuffer)} is called from {@code readContent}, after the {@code (CodecBuffer)}
 * constructor has run.
 */
public class UserLookupRequest extends RequestPacket {
    private String userName;

    public UserLookupRequest(@NotNull String userName) {
        this.userName = userName;
    }

    /**
     * Receiving side. Only call {@code super(buffer)}; the base class reads the query id and the payload is read
     * in {@link #readBuffer(CodecBuffer)}.
     */
    public UserLookupRequest(@NotNull CodecBuffer buffer) {
        super(buffer);
    }

    @Override
    public void writeBuffer(@NotNull CodecBuffer codecBuffer) {
        codecBuffer.writeString(userName);
    }

    @Override
    public void readBuffer(@NotNull CodecBuffer codecBuffer) {
        this.userName = codecBuffer.readString();
    }

    public String userName() {
        return userName;
    }
}

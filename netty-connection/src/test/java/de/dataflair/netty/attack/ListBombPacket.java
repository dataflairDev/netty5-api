package de.dataflair.netty5.attack;

import de.dataflair.netty5.common.codec.CodecBuffer;
import de.dataflair.netty5.common.packet.Packet;

import java.util.ArrayList;
import java.util.List;

/**
 * Stand-in for an application packet that reads a byte list. {@code CodecBuffer.readList} trusts the element
 * count and does not validate it against the readable bytes up front (unlike {@code readString}/{@code readBuffer}),
 * so this demonstrates the heap amplification an application packet can suffer.
 */
public class ListBombPacket extends Packet {
    private List<Byte> bytes = new ArrayList<>();

    public ListBombPacket() {
    }

    public ListBombPacket(CodecBuffer buffer) {
        super(buffer);
    }

    @Override
    protected void readContent(CodecBuffer buffer) {
        this.bytes = buffer.readByteList(new ArrayList<>());
    }

    public int size() {
        return bytes.size();
    }
}

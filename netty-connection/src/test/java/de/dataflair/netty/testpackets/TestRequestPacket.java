package de.dataflair.netty.testpackets;

import de.dataflair.netty.common.codec.CodecBuffer;
import de.dataflair.netty.common.packet.RequestPacket;

import java.util.ArrayList;
import java.util.List;

/**
 * Request with field initialisers: decoded values must not be overwritten by them.
 */
public class TestRequestPacket extends RequestPacket {
    private String tag = "initializer";
    private List<String> items = new ArrayList<>();

    public TestRequestPacket(String tag, List<String> items) {
        this.tag = tag;
        this.items = new ArrayList<>(items);
    }

    public TestRequestPacket(CodecBuffer buffer) {
        super(buffer);
    }

    @Override
    public void writeBuffer(CodecBuffer codecBuffer) {
        codecBuffer.writeString(tag).writeStringList(items);
    }

    @Override
    public void readBuffer(CodecBuffer codecBuffer) {
        this.tag = codecBuffer.readString();
        this.items = codecBuffer.readStringList(new ArrayList<>());
    }

    public String tag() {
        return tag;
    }

    public List<String> items() {
        return items;
    }
}

package de.dataflair.netty.common.codec;

import com.google.gson.JsonObject;
import de.dataflair.netty.NettyClientChannel;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class CodecBufferTest {

    enum Color { RED, GREEN, BLUE }

    record Point(int x, int y) {
    }

    record Label(String text) {
    }

    static final class PointSerializer extends BufferSerializer<Point> {
        PointSerializer() {
            super(Point.class);
        }

        @Override
        public void writeInBuffer(Point object, CodecBuffer codecBuffer) {
            codecBuffer.writeInt(object.x()).writeInt(object.y());
        }

        @Override
        public Point readFromBuffer(CodecBuffer codecBuffer) {
            return new Point(codecBuffer.readInt(), codecBuffer.readInt());
        }
    }

    static final class LabelSerializer extends BufferSerializer<Label> {
        LabelSerializer() {
            super(Label.class);
        }

        @Override
        public void writeInBuffer(Label object, CodecBuffer codecBuffer) {
            codecBuffer.writeString(object.text());
        }

        @Override
        public Label readFromBuffer(CodecBuffer codecBuffer) {
            return new Label(codecBuffer.readString());
        }
    }

    @Test
    void primitivesRoundTrip() {
        var uuid = UUID.randomUUID();
        var buffer = CodecBuffer.allocate()
                .writeString("hällo wörld")
                .writeBoolean(true)
                .writeUniqueId(uuid)
                .writeInt(-42)
                .writeEnum(Color.BLUE)
                .writeLong(Long.MIN_VALUE)
                .writeFloat(1.5f)
                .writeDouble(-2.25)
                .writeShort((short) 1234)
                .writeByte((byte) 7)
                .writeChar('Z')
                .writeCharSequence(new StringBuilder("seq"));

        assertEquals("hällo wörld", buffer.readString());
        assertTrue(buffer.readBoolean());
        assertEquals(uuid, buffer.readUniqueId());
        assertEquals(-42, buffer.readInt());
        assertEquals(Color.BLUE, buffer.readEnum(Color.class));
        assertEquals(Long.MIN_VALUE, buffer.readLong());
        assertEquals(1.5f, buffer.readFloat());
        assertEquals(-2.25, buffer.readDouble());
        assertEquals(1234, buffer.readShort());
        assertEquals(7, buffer.readByte());
        assertEquals('Z', buffer.readChar());
        assertEquals("seq", buffer.readString());
        assertEquals(0, buffer.origin().readableBytes());
    }

    @Test
    void nullableAndOptionalRoundTrip() {
        var buffer = CodecBuffer.allocate();
        buffer.writeNullable(null, b -> b.writeString("never"));
        buffer.writeNullable("x", b -> b.writeString("x"));
        buffer.writeOptional(Optional.empty(), b -> b.writeString("never"));
        buffer.writeOptional(Optional.of("y"), b -> b.writeString("y"));

        assertNull(buffer.readNullable(String.class, buffer::readString));
        assertEquals("x", buffer.readNullable(String.class, buffer::readString));
        assertEquals(Optional.empty(), buffer.readOptional(String.class, buffer::readString));
        assertEquals(Optional.of("y"), buffer.readOptional(String.class, buffer::readString));
        assertEquals(0, buffer.origin().readableBytes());
    }

    @Test
    void collectionsRoundTrip() {
        var map = new HashMap<String, Integer>();
        map.put("a", 1);
        map.put("b", 2);
        var buffer = CodecBuffer.allocate()
                .writeStringList(List.of("a", "b"))
                .writeIntegerList(List.of(1, 2, 3))
                .writeBooleanList(List.of(true, false))
                .writeByteList(List.of((byte) 1, (byte) -1))
                .writeList(List.of(10L, 20L), CodecBuffer::writeLong)
                .writeMap(map, CodecBuffer::writeString, CodecBuffer::writeInt)
                .writeBytes(new byte[]{9, 8, 7});

        assertEquals(List.of("a", "b"), buffer.readStringList(new ArrayList<>()));
        assertEquals(List.of(1, 2, 3), buffer.readIntegerList(new ArrayList<>()));
        assertEquals(List.of(true, false), buffer.readBooleanList(new ArrayList<>()));
        assertEquals(List.of((byte) 1, (byte) -1), buffer.readByteList(new ArrayList<>()));
        assertEquals(List.of(10L, 20L), buffer.readList(new ArrayList<>(), buffer::readLong));
        assertEquals(Map.of("a", 1, "b", 2), buffer.readMap(new HashMap<>(), buffer::readString, buffer::readInt));
        assertEquals(9, buffer.readByte());
        assertEquals(8, buffer.readByte());
        assertEquals(7, buffer.readByte());
    }

    @Test
    void streamAndJsonRoundTrip() {
        var identity = new NettyClientChannel.Identity("node", UUID.randomUUID());
        var json = new JsonObject();
        json.addProperty("key", "<value>");
        var buffer = CodecBuffer.allocate().writeStream(identity).writeJsonObject(json);

        var read = buffer.readStream(new NettyClientChannel.Identity());
        assertEquals(identity.name(), read.name());
        assertEquals(identity.uuid(), read.uuid());
        assertEquals(json, buffer.readJsonObject());
    }

    /**
     * {@code readBuffer} returns exactly the nested bytes, not the parent buffer.
     */
    @Test
    void readBufferReturnsSubsequenceOnly() {
        var nested = CodecBuffer.allocate().writeInt(1).writeInt(2).writeInt(3);
        var buffer = CodecBuffer.allocate().writeBuffer(nested).writeInt(42);

        var read = buffer.readBuffer();
        assertNotSame(buffer.origin(), read.origin());
        assertEquals(3 * Integer.BYTES, read.origin().readableBytes());
        assertEquals(1, read.readInt());
        assertEquals(2, read.readInt());
        assertEquals(3, read.readInt());
        assertEquals(0, read.origin().readableBytes());
        assertEquals(42, buffer.readInt());
    }

    @Test
    void readStringRejectsInvalidLength() {
        var buffer = CodecBuffer.allocate().writeInt(1000).writeByte((byte) 1);
        assertThrows(IndexOutOfBoundsException.class, buffer::readString);
    }

    /**
     * The serializer is chosen by the type it handles.
     */
    @Test
    void serializerIsFoundByHandledType() {
        CodecBuffer.addBufferSerializer(new LabelSerializer(), new PointSerializer());
        var buffer = CodecBuffer.allocate()
                .writeWithSerializer(new Point(3, 4), Point.class)
                .writeWithSerializer(new Label("hi"), Label.class);

        assertEquals(new Point(3, 4), buffer.readBySerializer(Point.class));
        assertEquals(new Label("hi"), buffer.readBySerializer(Label.class));
        assertEquals(0, buffer.origin().readableBytes());
    }

    @Test
    void missingSerializerWritesNothing() {
        var buffer = CodecBuffer.allocate().writeWithSerializer(UUID.randomUUID(), UUID.class);
        assertEquals(0, buffer.origin().readableBytes());
        assertNull(buffer.readBySerializer(UUID.class));
    }
}

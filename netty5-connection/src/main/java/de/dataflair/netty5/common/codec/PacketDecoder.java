package de.dataflair.netty5.common.codec;

/*
 * MIT License
 *
 * Copyright (c) 2024 02:04 Mario Pascal K.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

import de.dataflair.netty5.Netty5ChannelUtils;
import de.dataflair.netty5.Netty5ClientChannel;
import de.dataflair.netty5.common.packet.PacketFactory;
import de.dataflair.netty5.common.packet.auth.AuthPacket;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import org.jetbrains.annotations.Nullable;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Decodes one length-delimited frame into a packet.
 * <p>
 * Frame layout: {@code int nameLength, UTF-8 class name, int contentLength, content}.
 * The frame is released by {@link MessageToMessageDecoder}; the packet receives its own heap copy of the
 * content, so it stays valid after the frame has been released.
 * <p>
 * The class named in the frame is resolved through the {@link PacketRegistry} without initialisation and is
 * only instantiated if it is a concrete packet class. If {@code requireAuthentication} is set, any packet other
 * than {@link AuthPacket} is rejected (and the channel closed) until {@link Netty5ChannelUtils#AUTHENTICATED}
 * is set on the channel. Before authentication no class other than the auth packet is even loaded.
 */
public final class PacketDecoder extends MessageToMessageDecoder<ByteBuf> {
    /**
     * Maximum length of a class name in bytes.
     */
    public static final int MAX_CLASS_NAME_LENGTH = 1024;
    private static final String AUTH_PACKET_NAME = AuthPacket.class.getName();

    private final Netty5ClientChannel.Identity identity;
    private final PacketRegistry packetRegistry;
    private final boolean requireAuthentication;

    public PacketDecoder(@Nullable Netty5ClientChannel.Identity identity) {
        this(identity, null, false);
    }

    public PacketDecoder(@Nullable Netty5ClientChannel.Identity identity,
                         @Nullable PacketRegistry packetRegistry,
                         boolean requireAuthentication) {
        super(ByteBuf.class);
        this.identity = identity;
        this.packetRegistry = packetRegistry == null ? new PacketRegistry() : packetRegistry;
        this.requireAuthentication = requireAuthentication;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf frame, List<Object> out) {
        if (frame.readableBytes() < Integer.BYTES) {
            throw new PacketRejectedException(prefix() + "Frame too short: " + frame.readableBytes() + " bytes");
        }
        var nameLength = frame.readInt();
        if (nameLength <= 0 || nameLength > MAX_CLASS_NAME_LENGTH || nameLength > frame.readableBytes()) {
            throw new PacketRejectedException(prefix() + "Invalid class name length: " + nameLength);
        }
        var className = frame.readCharSequence(nameLength, StandardCharsets.UTF_8).toString();

        if (requireAuthentication && !isAuthenticated(ctx) && !AUTH_PACKET_NAME.equals(className)) {
            ctx.close();
            throw new PacketRejectedException(prefix() + "Packet before authentication rejected: " + className);
        }

        if (frame.readableBytes() < Integer.BYTES) {
            throw new PacketRejectedException(prefix() + "Missing content length for " + className);
        }
        var contentLength = frame.readInt();
        if (contentLength < 0 || contentLength > frame.readableBytes()) {
            throw new PacketRejectedException(prefix() + "Invalid content length " + contentLength + " for " + className);
        }

        var packetClass = packetRegistry.resolve(className);

        var content = Unpooled.buffer(contentLength);
        frame.readBytes(content, contentLength);
        if (frame.isReadable()) {
            System.err.println(prefix() + "Frame not empty after decoding " + className
                    + ", skipping " + frame.readableBytes() + " bytes");
        }

        try {
            out.add(PacketFactory.create(packetClass, new CodecBuffer(content)));
        } catch (ReflectiveOperationException | RuntimeException e) {
            throw new PacketRejectedException(prefix() + "Error while decoding packet " + className, e);
        }
    }

    private static boolean isAuthenticated(ChannelHandlerContext ctx) {
        return Boolean.TRUE.equals(ctx.channel().attr(Netty5ChannelUtils.AUTHENTICATED).get());
    }

    private String prefix() {
        return identity != null ? "[identity: " + identity.name() + "] " : "";
    }
}

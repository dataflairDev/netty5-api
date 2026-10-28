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
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.MessageToMessageDecoder;
import de.dataflair.netty5.common.log.SafeLog;
import de.dataflair.netty5.common.packet.Packet;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 * is set on the channel. Before authentication no class other than the auth packet is even loaded, and a frame
 * longer than {@code preAuthMaxFrameLength} (including the length field) closes the channel.
 * <p>
 * Any {@link Throwable} thrown while a packet class is resolved or constructed (for example an
 * {@link ExceptionInInitializerError}, {@link StackOverflowError} or {@link LinkageError}) drops the frame and
 * closes the channel. Frames that are still buffered when the channel has been closed are not decoded.
 */
public final class PacketDecoder extends MessageToMessageDecoder<ByteBuf> {
    /**
     * Maximum length of a class name in bytes.
     */
    public static final int MAX_CLASS_NAME_LENGTH = 1024;
    private static final Logger LOGGER = LoggerFactory.getLogger(PacketDecoder.class);
    private static final String AUTH_PACKET_NAME = AuthPacket.class.getName();

    private final Netty5ClientChannel.Identity identity;
    private final PacketRegistry packetRegistry;
    private final boolean requireAuthentication;
    private final int preAuthMaxFrameLength;

    public PacketDecoder(@Nullable Netty5ClientChannel.Identity identity) {
        this(identity, null, false);
    }

    public PacketDecoder(@Nullable Netty5ClientChannel.Identity identity,
                         @Nullable PacketRegistry packetRegistry,
                         boolean requireAuthentication) {
        this(identity, packetRegistry, requireAuthentication, Netty5ChannelUtils.DEFAULT_PRE_AUTH_MAX_FRAME_LENGTH);
    }

    /**
     * @param preAuthMaxFrameLength maximum frame length in bytes (including the length field) before
     *                              authentication; only used if {@code requireAuthentication} is set
     */
    public PacketDecoder(@Nullable Netty5ClientChannel.Identity identity,
                         @Nullable PacketRegistry packetRegistry,
                         boolean requireAuthentication,
                         int preAuthMaxFrameLength) {
        super(ByteBuf.class);
        if (preAuthMaxFrameLength <= 0) {
            throw new IllegalArgumentException("preAuthMaxFrameLength must be positive: " + preAuthMaxFrameLength);
        }
        this.identity = identity;
        this.packetRegistry = packetRegistry == null ? new PacketRegistry() : packetRegistry;
        this.requireAuthentication = requireAuthentication;
        this.preAuthMaxFrameLength = preAuthMaxFrameLength;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf frame, List<Object> out) {
        // a closed (for example rejected) channel processes nothing that was already buffered
        if (!ctx.channel().isActive()) {
            return;
        }
        if (requireAuthentication && !isAuthenticated(ctx)
                && (long) frame.readableBytes() + Integer.BYTES > preAuthMaxFrameLength) {
            ctx.close();
            throw new PacketRejectedException(prefix() + "Frame of " + (frame.readableBytes() + Integer.BYTES)
                    + " bytes before authentication exceeds " + preAuthMaxFrameLength + " bytes");
        }
        if (frame.readableBytes() < Integer.BYTES) {
            throw new PacketRejectedException(prefix() + "Frame too short: " + frame.readableBytes() + " bytes");
        }
        var nameLength = frame.readInt();
        if (nameLength <= 0 || nameLength > MAX_CLASS_NAME_LENGTH || nameLength > frame.readableBytes()) {
            throw new PacketRejectedException(prefix() + "Invalid class name length: " + nameLength);
        }
        var className = frame.readCharSequence(nameLength, StandardCharsets.UTF_8).toString();
        var safeName = SafeLog.clean(className);

        if (requireAuthentication && !isAuthenticated(ctx) && !AUTH_PACKET_NAME.equals(className)) {
            ctx.close();
            throw new PacketRejectedException(prefix() + "Packet before authentication rejected: " + safeName);
        }

        if (frame.readableBytes() < Integer.BYTES) {
            throw new PacketRejectedException(prefix() + "Missing content length for " + safeName);
        }
        var contentLength = frame.readInt();
        if (contentLength < 0 || contentLength > frame.readableBytes()) {
            throw new PacketRejectedException(prefix() + "Invalid content length " + contentLength + " for " + safeName);
        }

        final Class<? extends Packet> packetClass;
        try {
            packetClass = packetRegistry.resolve(className);
        } catch (PacketRejectedException e) {
            throw new PacketRejectedException(prefix() + SafeLog.clean(e.getMessage()), e.getCause());
        } catch (Throwable e) {
            throw fatal(ctx, safeName, e);
        }

        var content = Unpooled.buffer(contentLength);
        frame.readBytes(content, contentLength);
        if (frame.isReadable() && SafeLog.allow(ctx.channel())) {
            LOGGER.warn("{}Frame not empty after decoding {}, skipping {} bytes", prefix(), safeName,
                    frame.readableBytes());
        }

        final Object packet;
        try {
            packet = PacketFactory.create(packetClass, new CodecBuffer(content));
        } catch (ReflectiveOperationException | RuntimeException e) {
            throw new PacketRejectedException(prefix() + "Error while decoding packet " + safeName, e);
        } catch (Throwable e) {
            // ExceptionInInitializerError, StackOverflowError, LinkageError, ...: drop the frame, close
            throw fatal(ctx, safeName, e);
        }
        // rewind so a received packet can be forwarded unchanged
        content.readerIndex(0);
        out.add(packet);
    }

    private DecoderException fatal(ChannelHandlerContext ctx, String safeName, Throwable cause) {
        ctx.close();
        return new DecoderException(prefix() + "Fatal error while decoding packet " + safeName, cause);
    }

    private static boolean isAuthenticated(ChannelHandlerContext ctx) {
        return Boolean.TRUE.equals(ctx.channel().attr(Netty5ChannelUtils.AUTHENTICATED).get());
    }

    private String prefix() {
        return identity != null ? "[identity: " + identity.name() + "] " : "";
    }
}

package de.dataflair.netty5;

import de.dataflair.netty5.client.Netty5Client;
import de.dataflair.netty5.client.Netty5ClientHandler;
import de.dataflair.netty5.common.codec.PacketRegistry;
import de.dataflair.netty5.common.packet.Packet;
import de.dataflair.netty5.common.packet.auth.AuthPacket;
import de.dataflair.netty5.server.Netty5Server;
import de.dataflair.netty5.server.Netty5ServerHandler;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Which decoder errors close the channel in the server and client handler, and the connection future after
 * shutdown.
 */
class HandlerCloseTest {

    private static EmbeddedChannel serverChannel(Netty5Server server) {
        return new EmbeddedChannel(new Netty5ChannelInitializer(null, Netty5ChannelUtils.DEFAULT_MAX_FRAME_LENGTH,
                Netty5ChannelUtils.DEFAULT_PRE_AUTH_MAX_FRAME_LENGTH, new PacketRegistry().allowAnyPacketClass(), true) {
            @Override
            public SimpleChannelInboundHandler<?> handler() {
                return new Netty5ServerHandler(server);
            }
        });
    }

    private static EmbeddedChannel clientChannel(Netty5Client client) {
        return new EmbeddedChannel(new Netty5ChannelInitializer(null, Netty5ChannelUtils.DEFAULT_MAX_FRAME_LENGTH,
                new PacketRegistry().allowAnyPacketClass(), false) {
            @Override
            public SimpleChannelInboundHandler<?> handler() {
                return new Netty5ClientHandler(client);
            }
        });
    }

    /**
     * A length field with a negative value.
     */
    private static ByteBuf negativeLengthField() {
        return Unpooled.buffer().writeInt(-1).writeInt(0);
    }

    /**
     * A well-formed frame naming a class that does not exist.
     */
    private static ByteBuf unknownClassFrame() {
        var name = "com.example.DoesNotExist".getBytes(StandardCharsets.UTF_8);
        return Unpooled.buffer()
                .writeInt(Integer.BYTES + name.length + Integer.BYTES)
                .writeInt(name.length).writeBytes(name).writeInt(0);
    }

    /**
     * Encodes the packet with a separate codec and feeds the bytes into the given channel.
     */
    private static void feed(EmbeddedChannel target, Packet packet) {
        var encoder = new EmbeddedChannel(new Netty5ChannelInitializer(null) {
            @Override
            public SimpleChannelInboundHandler<?> handler() {
                return new SimpleChannelInboundHandler<Packet>() {
                    @Override
                    protected void channelRead0(io.netty.channel.ChannelHandlerContext ctx, Packet msg) {
                    }
                };
            }
        });
        assertTrue(encoder.writeOutbound(packet));
        ByteBuf out;
        while ((out = encoder.readOutbound()) != null) {
            target.writeInbound(out);
        }
        encoder.finishAndReleaseAll();
    }

    @Test
    void serverClosesOnNegativeLengthField() throws Exception {
        var server = new Netty5Server("127.0.0.1", 0);
        try {
            var unauthenticated = serverChannel(server);
            unauthenticated.writeInbound(negativeLengthField());
            assertFalse(unauthenticated.isOpen(), "unauthenticated channel must be closed");
            unauthenticated.finishAndReleaseAll();

            var authenticated = serverChannel(server);
            feed(authenticated, new AuthPacket(new Netty5ClientChannel.Identity("c", UUID.randomUUID()), Map.of()));
            assertEquals(Boolean.TRUE, authenticated.attr(Netty5ChannelUtils.AUTHENTICATED).get());
            authenticated.writeInbound(negativeLengthField());
            assertFalse(authenticated.isOpen(), "authenticated channel must be closed after a framing error");
            authenticated.finishAndReleaseAll();
        } finally {
            server.shutdownGracefully(0, 1, TimeUnit.SECONDS).await(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void serverKeepsAuthenticatedChannelOnRejectedPacket() throws Exception {
        var server = new Netty5Server("127.0.0.1", 0);
        try {
            var authenticated = serverChannel(server);
            feed(authenticated, new AuthPacket(new Netty5ClientChannel.Identity("c", UUID.randomUUID()), Map.of()));
            authenticated.writeInbound(unknownClassFrame());
            assertTrue(authenticated.isOpen(), "a rejected packet after authentication keeps the channel open");
            authenticated.finishAndReleaseAll();
        } finally {
            server.shutdownGracefully(0, 1, TimeUnit.SECONDS).await(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void clientClosesOnNegativeLengthFieldButKeepsChannelOnRejectedPacket() throws Exception {
        var client = new Netty5Client("127.0.0.1", 1, new Netty5ClientChannel.Identity("c", UUID.randomUUID()), null);
        try {
            var rejected = clientChannel(client);
            rejected.writeInbound(unknownClassFrame());
            assertTrue(rejected.isOpen(), "a rejected packet keeps the client channel open");

            rejected.writeInbound(negativeLengthField());
            assertFalse(rejected.isOpen(), "client channel must be closed after a framing error");
            rejected.finishAndReleaseAll();
        } finally {
            client.shutdownGracefully(0, 1, TimeUnit.SECONDS).await(5, TimeUnit.SECONDS);
        }
    }

    /**
     * A connection lost because of the shutdown does not replace the completed connection future with a new
     * one that would never complete.
     */
    @Test
    void connectionFutureIsNotReplacedOnShutdown() throws Exception {
        var client = new Netty5Client("127.0.0.1", 1, new Netty5ClientChannel.Identity("c", UUID.randomUUID()), null);
        var channel = clientChannel(client);
        var connected = client.connectionFuture();
        assertTrue(connected.isDone(), "channelActive completes the connection future");

        client.shutdownGracefully(0, 1, TimeUnit.SECONDS).await(5, TimeUnit.SECONDS);
        assertFalse(channel.isOpen());
        assertSame(connected, client.connectionFuture(), "connection future must not be replaced after shutdown");
        assertTrue(client.connectionFuture().isDone());
        channel.finishAndReleaseAll();
    }

    /**
     * Without shutdown a lost connection replaces the completed future with a pending one.
     */
    @Test
    void connectionFutureIsReplacedOnConnectionLoss() throws Exception {
        var client = new Netty5Client("127.0.0.1", 1, new Netty5ClientChannel.Identity("c", UUID.randomUUID()), null);
        try {
            var channel = clientChannel(client);
            var connected = client.connectionFuture();
            channel.close();
            assertNotSame(connected, client.connectionFuture());
            assertFalse(client.connectionFuture().isDone());
            channel.finishAndReleaseAll();
        } finally {
            client.shutdownGracefully(0, 1, TimeUnit.SECONDS).await(5, TimeUnit.SECONDS);
        }
    }
}

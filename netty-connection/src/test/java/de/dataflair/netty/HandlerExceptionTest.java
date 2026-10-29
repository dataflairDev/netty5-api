package de.dataflair.netty;

import de.dataflair.netty.client.NettyClient;
import de.dataflair.netty.client.NettyClientHandler;
import de.dataflair.netty.server.NettyServer;
import de.dataflair.netty.server.NettyServerHandler;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Exceptions without a message do not cause a NullPointerException in the handlers.
 */
class HandlerExceptionTest {

    @Test
    void serverHandlerHandlesExceptionWithoutMessage() throws Exception {
        var server = new NettyServer("127.0.0.1", 0);
        try {
            var channel = new EmbeddedChannel(new NettyServerHandler(server));
            channel.pipeline().fireExceptionCaught(new RuntimeException());
            assertDoesNotThrow(channel::checkException);
            // fail-closed: an unexpected exception before authentication closes the connection
            assertFalse(channel.isOpen());
            channel.finishAndReleaseAll();
        } finally {
            server.shutdownGracefully(0, 1, TimeUnit.SECONDS).await(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void clientHandlerHandlesExceptionWithoutMessage() throws Exception {
        var client = new NettyClient("127.0.0.1", 1, new NettyClientChannel.Identity("c", UUID.randomUUID()), null);
        try {
            var channel = new EmbeddedChannel(new NettyClientHandler(client));
            channel.pipeline().fireExceptionCaught(new IllegalStateException((String) null));
            assertDoesNotThrow(channel::checkException);
            channel.finishAndReleaseAll();
        } finally {
            client.shutdownGracefully(0, 1, TimeUnit.SECONDS).await(5, TimeUnit.SECONDS);
        }
    }
}

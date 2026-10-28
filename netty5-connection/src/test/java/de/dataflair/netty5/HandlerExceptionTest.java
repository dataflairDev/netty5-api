package de.dataflair.netty5;

import de.dataflair.netty5.client.Netty5Client;
import de.dataflair.netty5.client.Netty5ClientHandler;
import de.dataflair.netty5.server.Netty5Server;
import de.dataflair.netty5.server.Netty5ServerHandler;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Fix 15: exceptions without a message do not cause a NullPointerException in the handlers.
 */
class HandlerExceptionTest {

    @Test
    void serverHandlerHandlesExceptionWithoutMessage() throws Exception {
        var server = new Netty5Server("127.0.0.1", 0);
        try {
            var channel = new EmbeddedChannel(new Netty5ServerHandler(server));
            channel.pipeline().fireExceptionCaught(new RuntimeException());
            assertDoesNotThrow(channel::checkException);
            assertTrue(channel.isOpen());
            channel.finishAndReleaseAll();
        } finally {
            server.shutdownGracefully(0, 1, TimeUnit.SECONDS).await(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void clientHandlerHandlesExceptionWithoutMessage() throws Exception {
        var client = new Netty5Client("127.0.0.1", 1, new Netty5ClientChannel.Identity("c", UUID.randomUUID()), null);
        try {
            var channel = new EmbeddedChannel(new Netty5ClientHandler(client));
            channel.pipeline().fireExceptionCaught(new IllegalStateException((String) null));
            assertDoesNotThrow(channel::checkException);
            channel.finishAndReleaseAll();
        } finally {
            client.shutdownGracefully(0, 1, TimeUnit.SECONDS).await(5, TimeUnit.SECONDS);
        }
    }
}

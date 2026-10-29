package de.dataflair.netty.demo;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Runs the demo end to end so that it breaks the build when it stops working against the library.
 */
class DemoApplicationTest {

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void demoRunsThroughAndShutsDown() throws Exception {
        var result = DemoApplication.run();

        assertEquals("welcome demo-client", result.welcome());
        assertEquals(List.of("hello from the client"), result.chatOnServer());
        assertEquals(List.of("intruder"), result.rejectedClients());

        assertTrue(result.asyncLookup().found());
        assertEquals("Alice", result.asyncLookup().displayName());
        assertEquals(List.of("admin", "support"), result.asyncLookup().roles());

        assertNotNull(result.blockingLookup(), "blocking lookup timed out");
        assertEquals("Bob", result.blockingLookup().displayName());
        assertEquals(List.of("customer"), result.blockingLookup().roles());

        assertNotNull(result.unknownLookup(), "blocking lookup timed out");
        assertFalse(result.unknownLookup().found());

        assertTrue(result.serverToClientLookup().found());
        assertEquals("demo-client", result.serverToClientLookup().displayName());

        assertEquals("demo-client", result.disconnectedClient());
    }
}

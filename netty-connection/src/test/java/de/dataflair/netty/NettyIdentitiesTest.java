package de.dataflair.netty5;

import de.dataflair.netty5.common.codec.PacketSizeEstimator;
import de.dataflair.netty5.common.log.SafeLog;
import de.dataflair.netty5.testpackets.TestPacket;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.util.Locale;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Canonical names, derived UUIDs, the packet size estimator and the channel independent log throttle.
 */
class Netty5IdentitiesTest {

    @Test
    void compatibilityCharactersFoldToOneCanonicalName() {
        assertEquals("sam", Netty5Identities.canonicalName("ſam"));
        assertTrue(Netty5Identities.sameName("ſam", "sam"));
        assertEquals("kevin", Netty5Identities.canonicalName("Kevin"));
        assertTrue(Netty5Identities.sameName("Kevin", "kevin"));
        assertTrue(Netty5Identities.sameName("ALICE", "alice"));
        assertEquals(Netty5Identities.derivedUuid("ſam"), Netty5Identities.derivedUuid("SAM"));
        assertEquals(Netty5Identities.derivedUuid("Kevin"), Netty5Identities.derivedUuid("kevin"));
    }

    @Test
    void dottedCapitalIStaysDistinctAndDoesNotDependOnTheLocale() {
        // equalsIgnoreCase folds İ to i character by character; the canonical form keeps the combining dot
        assertTrue("İlker".equalsIgnoreCase("ilker"));
        assertEquals("i̇lker", Netty5Identities.canonicalName("İlker"));
        assertFalse(Netty5Identities.sameName("İlker", "ilker"));
        assertNotEquals(Netty5Identities.derivedUuid("İlker"), Netty5Identities.derivedUuid("ilker"));

        var previous = Locale.getDefault();
        try {
            Locale.setDefault(Locale.forLanguageTag("tr-TR"));
            // with toLowerCase() the Turkish locale would turn "I" into a dotless "ı"
            assertEquals("ilker", Netty5Identities.canonicalName("ILKER"));
            assertEquals("i̇lker", Netty5Identities.canonicalName("İlker"));
        } finally {
            Locale.setDefault(previous);
        }
    }

    @Test
    void derivedUuidIsStableAndNamespaced() {
        assertEquals(Netty5Identities.derivedUuid("bob"), Netty5Identities.derivedUuid("Bob"));
        assertNotEquals(Netty5Identities.derivedUuid("bob"), Netty5Identities.derivedUuid("alice"));
        assertNotEquals(java.util.UUID.nameUUIDFromBytes("bob".getBytes(java.nio.charset.StandardCharsets.UTF_8)),
                Netty5Identities.derivedUuid("bob"));
        assertEquals(3, Netty5Identities.derivedUuid("bob").version());
    }

    @Test
    void packetSizeEstimatorCountsTheContent() {
        var handle = PacketSizeEstimator.INSTANCE.newHandle();
        var packet = new TestPacket("y".repeat(1_000), 1);
        var content = packet.buffer().origin().readableBytes();
        assertTrue(content > 1_000);
        assertEquals(content + PacketSizeEstimator.OVERHEAD, handle.size(packet));
        var buffer = Unpooled.buffer().writeZero(77);
        try {
            assertEquals(77, handle.size(buffer));
        } finally {
            buffer.release();
        }
    }

    @Test
    void channelIndependentLogThrottle() {
        var throttle = SafeLog.throttle();
        for (int i = 0; i < SafeLog.LINES_PER_WINDOW; i++) {
            assertTrue(throttle.allow());
        }
        assertFalse(throttle.allow());
    }
}

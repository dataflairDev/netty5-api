package de.dataflair.netty;

import de.dataflair.netty.common.codec.PacketSizeEstimator;
import de.dataflair.netty.common.log.SafeLog;
import de.dataflair.netty.testpackets.TestPacket;
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
class NettyIdentitiesTest {

    @Test
    void compatibilityCharactersFoldToOneCanonicalName() {
        assertEquals("sam", NettyIdentities.canonicalName("ſam"));
        assertTrue(NettyIdentities.sameName("ſam", "sam"));
        assertEquals("kevin", NettyIdentities.canonicalName("Kevin"));
        assertTrue(NettyIdentities.sameName("Kevin", "kevin"));
        assertTrue(NettyIdentities.sameName("ALICE", "alice"));
        assertEquals(NettyIdentities.derivedUuid("ſam"), NettyIdentities.derivedUuid("SAM"));
        assertEquals(NettyIdentities.derivedUuid("Kevin"), NettyIdentities.derivedUuid("kevin"));
    }

    @Test
    void dottedCapitalIStaysDistinctAndDoesNotDependOnTheLocale() {
        // equalsIgnoreCase folds İ to i character by character; the canonical form keeps the combining dot
        assertTrue("İlker".equalsIgnoreCase("ilker"));
        assertEquals("i̇lker", NettyIdentities.canonicalName("İlker"));
        assertFalse(NettyIdentities.sameName("İlker", "ilker"));
        assertNotEquals(NettyIdentities.derivedUuid("İlker"), NettyIdentities.derivedUuid("ilker"));

        var previous = Locale.getDefault();
        try {
            Locale.setDefault(Locale.forLanguageTag("tr-TR"));
            // with toLowerCase() the Turkish locale would turn "I" into a dotless "ı"
            assertEquals("ilker", NettyIdentities.canonicalName("ILKER"));
            assertEquals("i̇lker", NettyIdentities.canonicalName("İlker"));
        } finally {
            Locale.setDefault(previous);
        }
    }

    @Test
    void derivedUuidIsStableAndNamespaced() {
        assertEquals(NettyIdentities.derivedUuid("bob"), NettyIdentities.derivedUuid("Bob"));
        assertNotEquals(NettyIdentities.derivedUuid("bob"), NettyIdentities.derivedUuid("alice"));
        assertNotEquals(java.util.UUID.nameUUIDFromBytes("bob".getBytes(java.nio.charset.StandardCharsets.UTF_8)),
                NettyIdentities.derivedUuid("bob"));
        assertEquals(3, NettyIdentities.derivedUuid("bob").version());
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

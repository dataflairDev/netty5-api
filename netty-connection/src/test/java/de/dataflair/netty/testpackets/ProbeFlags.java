package de.dataflair.netty.testpackets;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Flags set by the static initialisers of the probe classes.
 */
public final class ProbeFlags {
    public static final AtomicBoolean NOT_A_PACKET_INITIALIZED = new AtomicBoolean();
    public static final AtomicBoolean ABSTRACT_PACKET_INITIALIZED = new AtomicBoolean();
    public static final AtomicBoolean PRE_AUTH_PACKET_INITIALIZED = new AtomicBoolean();
    public static final AtomicBoolean RESTRICTED_PACKET_INITIALIZED = new AtomicBoolean();
    public static final AtomicBoolean NON_PUBLIC_PACKET_INITIALIZED = new AtomicBoolean();

    private ProbeFlags() {
    }
}

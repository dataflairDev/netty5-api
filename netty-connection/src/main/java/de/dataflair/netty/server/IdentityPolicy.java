package de.dataflair.netty.server;

/**
 * What the server does when a client authenticates with a name (case-insensitive) or UUID that an authenticated
 * connection already uses.
 */
public enum IdentityPolicy {
    /**
     * The new connection is closed, the existing one stays (default).
     */
    REJECT,
    /**
     * The existing connection is closed and replaced by the new one.
     */
    REPLACE_OLD
}

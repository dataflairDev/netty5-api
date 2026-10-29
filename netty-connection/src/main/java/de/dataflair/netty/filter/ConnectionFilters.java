package de.dataflair.netty5.filter;

import org.jetbrains.annotations.Nullable;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Helpers for connection filters.
 */
public final class ConnectionFilters {
    private ConnectionFilters() {
    }

    /**
     * Compares a token received from a peer with the expected one in constant time. Both values are hashed with
     * SHA-256 first, so neither the content nor the length of the expected token leaks through the time the
     * comparison takes.
     *
     * @return true if both are non-null and equal
     */
    public static boolean tokenEquals(@Nullable String expected, @Nullable String actual) {
        if (expected == null || actual == null) {
            return false;
        }
        return constantTimeEquals(sha256(expected), sha256(actual));
    }

    /**
     * @return true if both arrays are non-null and equal, compared with {@link MessageDigest#isEqual}
     */
    public static boolean constantTimeEquals(byte @Nullable [] expected, byte @Nullable [] actual) {
        if (expected == null || actual == null) {
            return false;
        }
        return MessageDigest.isEqual(expected, actual);
    }

    private static byte[] sha256(String value) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }
}

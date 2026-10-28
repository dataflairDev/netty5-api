package de.dataflair.netty5;

import org.jetbrains.annotations.NotNull;

import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.util.Locale;
import java.util.UUID;

/**
 * Canonical client names and the UUIDs the server derives from them.
 * <p>
 * The server compares, looks up and derives UUIDs only from the canonical form of a name: Unicode NFKC followed
 * by {@code toLowerCase(Locale.ROOT)}. So {@code "ſam"} and {@code "Sam"} are the same identity ({@code "sam"}),
 * the Kelvin sign {@code "Kevin"} is {@code "kevin"}, and the result does not depend on the default locale.
 * A {@link de.dataflair.netty5.filter.ConnectionFilter} that binds a token to a name has to check the token
 * against {@link #canonicalName(String)} (or {@link Netty5ClientChannel.AuthType#canonicalName()}), otherwise
 * two spellings of one canonical name could end up with different tokens.
 */
public final class Netty5Identities {
    /**
     * Fixed prefix of the bytes a UUID is derived from, see {@link #derivedUuid(String)}.
     */
    public static final String UUID_NAMESPACE = "netty5-api:identity:";

    private Netty5Identities() {
    }

    /**
     * @return NFKC of {@code name}, lower-cased with {@link Locale#ROOT}
     */
    public static @NotNull String canonicalName(@NotNull String name) {
        return Normalizer.normalize(name, Normalizer.Form.NFKC).toLowerCase(Locale.ROOT);
    }

    /**
     * @return the UUID the server assigns to a client with this name (unless it trusts client UUIDs):
     * {@code UUID.nameUUIDFromBytes(UTF-8 of UUID_NAMESPACE + canonicalName(name))}
     */
    public static @NotNull UUID derivedUuid(@NotNull String name) {
        return UUID.nameUUIDFromBytes((UUID_NAMESPACE + canonicalName(name)).getBytes(StandardCharsets.UTF_8));
    }

    /**
     * @return true if both names have the same canonical form
     */
    public static boolean sameName(@NotNull String first, @NotNull String second) {
        return canonicalName(first).equals(canonicalName(second));
    }
}

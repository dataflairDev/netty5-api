package de.dataflair.netty5.common.codec;

import de.dataflair.netty5.common.packet.Packet;
import de.dataflair.netty5.common.packet.auth.AuthPacket;
import de.dataflair.netty5.common.packet.json.JsonPacket;
import de.dataflair.netty5.common.packet.json.JsonRequestPacket;
import de.dataflair.netty5.common.packet.json.JsonRespondPacket;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Modifier;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Resolves the class names received on the wire to packet classes.
 * <p>
 * By default every public, concrete {@link Packet} subclass with a public {@code (CodecBuffer)} constructor that
 * is visible to the configured class loader is accepted. The class is loaded <b>without initialisation</b> and is
 * only instantiated after these checks passed. With {@link #restrictToRegistered(boolean)} the registry acts as an
 * allowlist and only classes passed to {@link #register(Class[])} are accepted; the built-in packets of this
 * library are always registered.
 */
public final class PacketRegistry {
    private final Map<String, Class<? extends Packet>> registered = new ConcurrentHashMap<>();
    private final Map<String, Class<? extends Packet>> resolved = new ConcurrentHashMap<>();
    private final ClassLoader classLoader;
    private volatile boolean restrictToRegistered;

    public PacketRegistry() {
        this(PacketRegistry.class.getClassLoader());
    }

    public PacketRegistry(@NotNull ClassLoader classLoader) {
        this.classLoader = classLoader;
        register(AuthPacket.class, JsonPacket.class, JsonRequestPacket.class, JsonRespondPacket.class);
    }

    /**
     * Registers packet classes. Registered classes are accepted in both modes.
     *
     * @throws IllegalArgumentException if a class is abstract, not public or has no public {@code (CodecBuffer)}
     *                                  constructor
     */
    @SafeVarargs
    public final PacketRegistry register(@NotNull Class<? extends Packet>... packetClasses) {
        for (var packetClass : packetClasses) {
            var problem = validate(packetClass);
            if (problem != null) {
                throw new IllegalArgumentException(packetClass.getName() + ": " + problem);
            }
            registered.put(packetClass.getName(), packetClass);
        }
        return this;
    }

    /**
     * @param restrict if true only registered classes are accepted (allowlist mode)
     */
    public PacketRegistry restrictToRegistered(boolean restrict) {
        this.restrictToRegistered = restrict;
        return this;
    }

    public boolean restrictToRegistered() {
        return restrictToRegistered;
    }

    public boolean isRegistered(@NotNull String className) {
        return registered.containsKey(className);
    }

    /**
     * Resolves a class name to a packet class. A class that fails the checks is never initialised.
     *
     * @throws PacketRejectedException if the class must not be instantiated
     */
    public @NotNull Class<? extends Packet> resolve(@NotNull String className) {
        var packetClass = registered.get(className);
        if (packetClass != null) {
            return packetClass;
        }
        if (restrictToRegistered) {
            throw new PacketRejectedException("Packet class not registered: " + className);
        }
        packetClass = resolved.get(className);
        if (packetClass != null) {
            return packetClass;
        }

        final Class<?> candidate;
        try {
            candidate = Class.forName(className, false, classLoader);
        } catch (ClassNotFoundException | LinkageError e) {
            throw new PacketRejectedException("Unknown packet class: " + className, e);
        }
        if (!Packet.class.isAssignableFrom(candidate)) {
            throw new PacketRejectedException("Not a packet class: " + className);
        }
        var packetCandidate = candidate.asSubclass(Packet.class);
        var problem = validate(packetCandidate);
        if (problem != null) {
            throw new PacketRejectedException(className + ": " + problem);
        }
        resolved.put(className, packetCandidate);
        return packetCandidate;
    }

    private static String validate(Class<? extends Packet> packetClass) {
        var modifiers = packetClass.getModifiers();
        if (packetClass.isInterface() || Modifier.isAbstract(modifiers)) {
            return "packet class is abstract";
        }
        if (!Modifier.isPublic(modifiers)) {
            return "packet class is not public";
        }
        try {
            packetClass.getConstructor(CodecBuffer.class);
        } catch (NoSuchMethodException e) {
            return "no public constructor (CodecBuffer)";
        }
        return null;
    }
}

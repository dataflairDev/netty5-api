package de.dataflair.netty5.common.packet;



/*
 * Copyright 2023-2024 netty5-api contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import de.dataflair.netty5.Netty5ClientChannel;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.EventExecutor;
import lombok.Getter;
import lombok.Setter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.security.SecureRandom;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Sends packets and dispatches received packets to listeners, responders and pending requests.
 * <p>
 * All registries are thread-safe. Pending requests are completed by the event loop that receives the response;
 * they time out after {@link #requestTimeoutMillis()} milliseconds.
 */
@Getter
public abstract class Netty5PacketTransmitter {
    /**
     * Default timeout for requests in milliseconds.
     */
    public static final long DEFAULT_REQUEST_TIMEOUT_MILLIS = 5_000;
    private static final String KEY_ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

    private final SecureRandom keyRandom = new SecureRandom();
    private final EventLoopGroup eventExecutors;
    private final Consumer<Packet> packetConsumer;
    private final Map<Class<? extends Packet>, Map<String, BiConsumer<Netty5ClientChannel, Packet>>> listener = new ConcurrentHashMap<>();
    private final Map<UUID, CompletableFuture<Packet>> pendingRequests = new ConcurrentHashMap<>();
    private final Map<Class<? extends RequestPacket>, Map<String, Function<RequestPacket, RespondPacket>>> responders = new ConcurrentHashMap<>();
    @Setter
    private volatile long requestTimeoutMillis = DEFAULT_REQUEST_TIMEOUT_MILLIS;

    protected Netty5PacketTransmitter(@NotNull EventLoopGroup eventExecutors,
                                      @NotNull Consumer<Packet> packetConsumer) {
        this.eventExecutors = eventExecutors;
        this.packetConsumer = packetConsumer;
    }

    @SuppressWarnings("unchecked")
    public <P extends Packet> void listen(@NotNull Class<P> packetClass,
                                          @NotNull String key,
                                          @NotNull BiConsumer<Netty5ClientChannel, P> callback) {
        this.listener.computeIfAbsent(packetClass, _ -> new ConcurrentHashMap<>())
                .put(key, (BiConsumer<Netty5ClientChannel, Packet>) callback);
    }

    public <P extends Packet> void listen(@NotNull Class<P> packetClass,
                                          @NotNull BiConsumer<Netty5ClientChannel, P> callback) {
        this.listen(packetClass, generateRandomKey(), callback);
    }

    @SuppressWarnings("unchecked")
    public <Q extends RequestPacket> void listenQuery(@NotNull Class<Q> queryClass,
                                                    @NotNull String key,
                                                    @NotNull Function<Q, RespondPacket> callback) {
        this.responders.computeIfAbsent(queryClass, _ -> new ConcurrentHashMap<>())
                .put(key, (Function<RequestPacket, RespondPacket>) callback);
    }

    public <Q extends RequestPacket> void listenQuery(@NotNull Class<Q> queryClass,
                                                    @NotNull Function<Q, RespondPacket> callback) {
        this.listenQuery(queryClass, generateRandomKey(), callback);
    }

    public void unregisterListener(@NotNull Class<? extends Packet> packetClass,
                                   @NotNull String key) {
        var listeners = this.listener.get(packetClass);
        if (listeners != null) {
            listeners.remove(key);
        }
    }

    /**
     * Removes a responder registered with {@link #listenQuery(Class, String, Function)}.
     *
     * @param requestClass the request class the responder was registered for
     */
    public void unregisterResponder(@NotNull Class<? extends RequestPacket> requestClass,
                                    @NotNull String key) {
        var registered = this.responders.get(requestClass);
        if (registered != null) {
            registered.remove(key);
        }
    }

    public void publishPacket(@NotNull Packet packet) {
        packetConsumer.accept(packet);
    }

    /**
     * Sends a request and calls {@code callback} with the response. If the response is not of type
     * {@code packetClass} or does not arrive within the request timeout, the callback is not called and the
     * failure is logged; use {@link #queryPacket(RequestPacket, Class)} to observe failures.
     */
    public <P extends Packet> void queryPacket(@NotNull RequestPacket requestPacket,
                                               @NotNull Class<P> packetClass,
                                               @NotNull Consumer<P> callback) {
        this.query(requestPacket, packetClass).whenComplete((response, throwable) -> {
            if (throwable != null) {
                System.err.println("Request " + requestPacket.getClass().getName() + " failed: " + throwable);
                return;
            }
            callback.accept(response);
        });
    }

    /**
     * Sends a request and blocks until the response arrives or the request timeout elapses.
     * Must not be called from an event loop thread of this transmitter, because the response is processed there.
     *
     * @return the response, or null on timeout, type mismatch or interruption
     * @throws IllegalStateException if called from an event loop thread
     */
    public <P extends Packet> @Nullable P queryPacketDirect(@NotNull RequestPacket requestPacket,
                                                            @NotNull Class<P> packetClass) {
        if (inEventLoop()) {
            throw new IllegalStateException("queryPacketDirect must not be called from an event loop thread; "
                    + "use queryPacket(...) with a callback or CompletableFuture instead");
        }
        var future = this.query(requestPacket, packetClass);
        try {
            return future.get(requestTimeoutMillis + 1_000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        } catch (ExecutionException | TimeoutException e) {
            return null;
        }
    }

    /**
     * Sends a request. The returned future completes with the response, or exceptionally with a
     * {@link ClassCastException} if the response is not of type {@code packetClass} or a
     * {@link TimeoutException} after the request timeout.
     */
    @SuppressWarnings("unchecked")
    public <P extends Packet> CompletableFuture<Packet> queryPacket(@NotNull RequestPacket requestPacket,
                                                                    @NotNull Class<P> packetClass) {
        return (CompletableFuture<Packet>) this.query(requestPacket, packetClass);
    }

    /**
     * Registers a pending request and sends it.
     */
    protected <P extends Packet> CompletableFuture<P> query(@NotNull RequestPacket requestPacket,
                                                            @NotNull Class<P> packetClass) {
        var queryId = UUID.randomUUID();
        requestPacket.queryId = queryId;
        requestPacket.buffer.writeUniqueId(queryId);
        requestPacket.writeBuffer(requestPacket.buffer);

        var pending = new CompletableFuture<Packet>();
        pendingRequests.put(queryId, pending);
        pending.orTimeout(requestTimeoutMillis, TimeUnit.MILLISECONDS)
                .whenComplete((_, _) -> pendingRequests.remove(queryId, pending));

        var typed = pending.thenApply(response -> {
            if (!packetClass.isInstance(response)) {
                throw new ClassCastException("Expected " + packetClass.getName() + " but received "
                        + response.getClass().getName());
            }
            return packetClass.cast(response);
        });

        try {
            this.publishPacket(requestPacket);
        } catch (RuntimeException e) {
            pending.completeExceptionally(e);
        }
        return typed;
    }

    /**
     * Completes the pending request matching the query id of the given response.
     *
     * @return true if a pending request was completed
     */
    public boolean completeRequest(@NotNull RespondPacket respondPacket) {
        var queryId = respondPacket.queryId();
        if (queryId == null) {
            return false;
        }
        var pending = pendingRequests.remove(queryId);
        return pending != null && pending.complete(respondPacket);
    }

    /**
     * @return true if the current thread is an event loop thread of this transmitter's event loop group
     */
    protected boolean inEventLoop() {
        for (EventExecutor executor : eventExecutors) {
            if (executor.inEventLoop()) {
                return true;
            }
        }
        return false;
    }

    public abstract void callActions(@NotNull Packet packet, Netty5ClientChannel sender);

    public void call(@NotNull Packet packet, @Nullable Netty5ClientChannel sender) {
        if (packet instanceof RespondPacket respondPacket) {
            this.completeRequest(respondPacket);
        }

        if (packet instanceof RequestPacket requestPacket) {
            this.callResponder(requestPacket);
        }

        this.callActions(packet, sender);
        this.callListeners(packet, sender);
    }

    protected void callListeners(@NotNull Packet packet, @Nullable Netty5ClientChannel sender) {
        var listeners = listener.get(packet.getClass());
        if (listeners != null) {
            listeners.forEach((_, packetConsumer) -> packetConsumer.accept(sender, packet));
        }
    }

    protected String generateRandomKey() {
        var key = new StringBuilder(10);
        for (var i = 0; i < 10; i++) {
            key.append(KEY_ALPHABET.charAt(keyRandom.nextInt(KEY_ALPHABET.length())));
        }
        return key.toString();
    }

    public <R extends RequestPacket> void callResponder(@NotNull R request) {
        var registered = responders.get(request.getClass());
        if (registered != null) {
            registered.forEach((_, packetRespondPacketFunction) ->
                    publishPacket(createResponse(request, packetRespondPacketFunction)));
        }
    }

    protected static RespondPacket createResponse(@NotNull RequestPacket request,
                                                  @NotNull Function<RequestPacket, RespondPacket> responder) {
        var respondPacket = responder.apply(request);
        respondPacket.queryId(request.queryId);
        respondPacket.buffer.writeUniqueId(request.queryId);
        respondPacket.writeBuffer(respondPacket.buffer);
        return respondPacket;
    }
}

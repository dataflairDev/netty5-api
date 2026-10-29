package de.dataflair.netty5.filter;

import de.dataflair.netty5.Netty5ClientChannel;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.CompletionStage;

/**
 * Connection filter that decides asynchronously, for example after a database or HTTP lookup.
 * <p>
 * While the returned stage is pending the server does not read from the channel ({@code autoRead=false}); the
 * result is processed on the channel's event loop. A stage that completes exceptionally or with anything other
 * than {@code true} rejects the connection. The authentication timeout keeps running, so a stage that never
 * completes ends with the timeout.
 * <p>
 * Synchronous {@link ConnectionFilter}s, {@link PacketReceiveFilter}s, listeners and responders run on the
 * event loop and must not block. Anything that waits for I/O belongs in an {@code AsyncConnectionFilter} (or
 * on an executor of the application).
 */
public abstract class AsyncConnectionFilter {
    /**
     * Must not block; return a stage that completes later instead.
     *
     * @return a stage completing with {@code true} to accept the connection
     */
    public abstract @NotNull CompletionStage<Boolean> evaluateAsync(@NotNull Netty5ClientChannel.AuthType auth);
}

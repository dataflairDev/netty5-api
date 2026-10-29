package de.dataflair.netty5.actions;

import de.dataflair.netty5.Netty5ClientChannel;
import lombok.Getter;
import org.jetbrains.annotations.NotNull;

import java.util.function.Consumer;

@Getter
public class ConnectionAction extends Action<Netty5ClientChannel> {
    private final State state;

    public ConnectionAction(@NotNull Consumer<Netty5ClientChannel> consumer, @NotNull State state) {
        super(consumer);
        this.state = state;
    }

    public enum State {
        CLIENT_AUTHENTICATED,
        CLIENT_DISCONNECTED
    }
}

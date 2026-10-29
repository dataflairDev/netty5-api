package de.dataflair.netty.actions;

import de.dataflair.netty.NettyClientChannel;
import lombok.Getter;
import org.jetbrains.annotations.NotNull;

import java.util.function.Consumer;

@Getter
public class ConnectionAction extends Action<NettyClientChannel> {
    private final State state;

    public ConnectionAction(@NotNull Consumer<NettyClientChannel> consumer, @NotNull State state) {
        super(consumer);
        this.state = state;
    }

    public enum State {
        CLIENT_AUTHENTICATED,
        CLIENT_DISCONNECTED
    }
}

package de.dataflair.netty5.actions;

import lombok.Getter;
import org.jetbrains.annotations.NotNull;

import java.util.function.Consumer;

@Getter
public abstract class Action<T> {
    private final Consumer<T> consumer;

    protected Action(@NotNull Consumer<T> consumer) {
        this.consumer = consumer;
    }
}

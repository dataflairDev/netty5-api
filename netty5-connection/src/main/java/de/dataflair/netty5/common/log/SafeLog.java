package de.dataflair.netty5.common.log;

import io.netty.channel.Channel;
import io.netty.util.AttributeKey;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Helpers for log messages that contain values chosen by the peer (identity names, class names, auth
 * properties, exception messages).
 * <ul>
 *     <li>{@link #clean(Object)} cuts a value to {@value #MAX_LENGTH} characters and replaces control characters
 *     (including CR, LF and the Unicode line separators) with {@code '?'}, so a peer can neither forge log lines
 *     nor blow up the log with one huge value.</li>
 *     <li>{@link #allow(Channel)} throttles log lines per channel: at most {@value #LINES_PER_WINDOW} lines per
 *     {@value #WINDOW_MILLIS} ms, the rest is dropped.</li>
 * </ul>
 */
public final class SafeLog {
    /**
     * Maximum length of a peer controlled value in a log message.
     */
    public static final int MAX_LENGTH = 200;
    /**
     * Log lines allowed per channel and window.
     */
    public static final int LINES_PER_WINDOW = 10;
    /**
     * Length of the throttling window in milliseconds.
     */
    public static final long WINDOW_MILLIS = 10_000;

    private static final AttributeKey<Window> WINDOW = AttributeKey.valueOf("netty5-api.log-window");

    private SafeLog() {
    }

    /**
     * @return the value as a single line of at most {@value #MAX_LENGTH} characters, {@code "null"} for null
     */
    public static @NotNull String clean(@Nullable Object value) {
        var text = String.valueOf(value);
        var cut = text.length() > MAX_LENGTH;
        var length = cut ? MAX_LENGTH : text.length();
        var builder = new StringBuilder(length + 3);
        for (var i = 0; i < length; i++) {
            var c = text.charAt(i);
            builder.append(Character.isISOControl(c) || c == ' ' || c == ' ' ? '?' : c);
        }
        if (cut) {
            builder.append("...");
        }
        return builder.toString();
    }

    /**
     * @return true if another log line for this channel may be written in the current window
     */
    public static boolean allow(@Nullable Channel channel) {
        if (channel == null) {
            return true;
        }
        var window = channel.attr(WINDOW).get();
        if (window == null) {
            window = new Window();
            var existing = channel.attr(WINDOW).setIfAbsent(window);
            if (existing != null) {
                window = existing;
            }
        }
        return window.allow(System.currentTimeMillis());
    }

    private static final class Window {
        private long start;
        private int lines;

        synchronized boolean allow(long now) {
            if (now - start >= WINDOW_MILLIS) {
                start = now;
                lines = 0;
            }
            return ++lines <= LINES_PER_WINDOW;
        }
    }
}

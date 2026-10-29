package de.dataflair.netty.filter;

import de.dataflair.netty.NettyClientChannel;

/**
 * Decides whether an authenticating client is accepted. Runs on the event loop and must not block; use an
 * {@link AsyncConnectionFilter} for lookups that wait for I/O. Compare secrets with
 * {@link ConnectionFilters#tokenEquals(String, String)} and bind the token to the identity the client claims
 * (for example a token per allowed name), otherwise every token holder can use any name.
 */
public abstract class ConnectionFilter extends Filter<NettyClientChannel.AuthType> {
}
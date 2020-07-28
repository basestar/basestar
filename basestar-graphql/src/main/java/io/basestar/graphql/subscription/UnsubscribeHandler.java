package io.basestar.graphql.subscription;

import io.basestar.auth.Caller;

import java.util.concurrent.CompletableFuture;

// Compatible with Broadcaster::unsubscribe

public interface UnsubscribeHandler {

    CompletableFuture<?> unsubscribe(Caller caller, String sub, String channel);

    default CompletableFuture<?> unsubscribeAll(final Caller caller, final String sub) {

        return unsubscribe(caller, sub, null);
    }
}

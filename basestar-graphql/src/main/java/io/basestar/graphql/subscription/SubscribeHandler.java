package io.basestar.graphql.subscription;

import io.basestar.auth.Caller;
import io.basestar.expression.Expression;
import io.basestar.util.Name;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

// Compatible with Broadcaster::subscribe

public interface SubscribeHandler {

    CompletableFuture<?> subscribe(Caller caller, String sub, String channel, String schema, Expression expression, Set<Name> expand);
}

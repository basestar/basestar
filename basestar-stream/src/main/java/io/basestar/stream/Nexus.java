package io.basestar.stream;

import io.basestar.auth.Caller;
import io.basestar.expression.Expression;
import io.basestar.util.Path;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

public interface Nexus {

    CompletableFuture<?> subscribe(Caller caller, String sub, String channel, String schema, Expression expression, Set<Path> expand);

    CompletableFuture<?> unsubscribe(Caller caller, String sub, String channel);

    CompletableFuture<?> unsubscribeAll(Caller caller, String sub);
}
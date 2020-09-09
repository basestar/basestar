package io.basestar.stream;

import io.basestar.auth.Caller;
import io.basestar.expression.Expression;

import java.util.concurrent.CompletableFuture;

public interface Subscribable {

    CompletableFuture<?> subscribe(Caller caller, String sub, String channel, String schema, Expression expression, SubscriptionInfo info);

    CompletableFuture<?> unsubscribe(Caller caller, String sub, String channel);

    CompletableFuture<?> unsubscribeAll(Caller caller, String sub);
}

package io.basestar.stream;

import io.basestar.auth.Caller;
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.schema.LinkableSchema;
import io.basestar.util.Page;
import io.basestar.util.Pager;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Simple in-memory storage for subscriptiona
 */

public class MemorySubscriptions implements Subscriptions {

    private final List<Subscription> subscriptions = new ArrayList<>();

    private final Object lock = new Object();

    @Override
    public CompletableFuture<?> subscribe(final Caller caller, final String sub, final String channel, final LinkableSchema schema, final Set<Change.Event> events, final Expression expression, final SubscriptionMetadata metadata) {

        final Subscription subscription = new Subscription();
        subscription.setCaller(caller);
        subscription.setSub(sub);
        subscription.setChannel(channel);
        subscription.setSchema(schema.getQualifiedName());
        subscription.setEvents(events);
        subscription.setExpression(expression);
        subscription.setMetadata(metadata);
        synchronized (lock) {
            subscriptions.removeIf(v -> v.matches(sub, channel));
            subscriptions.add(subscription);
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public Pager<Subscription> query(final LinkableSchema schema, final Change.Event event, final Map<String, Object> before, final Map<String, Object> after) {

        final List<Subscription> results = new ArrayList<>();
        synchronized (lock) {
            subscriptions.forEach(v -> {
                if(matches(v, schema, event, before, after)) {
                    results.add(v);
                }
            });
        }
        return (stats, token, count) -> CompletableFuture.completedFuture(Page.from(results));
    }

    @Override
    public CompletableFuture<?> unsubscribe(final String sub, final String channel) {

        synchronized (lock) {
            subscriptions.removeIf(v -> v.matches(sub, channel));
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<?> unsubscribeAll(final String sub) {

        synchronized (lock) {
            subscriptions.removeIf(v -> v.matches(sub));
        }
        return CompletableFuture.completedFuture(null);
    }

    private boolean matches(final Subscription subscription, final LinkableSchema schema, final Change.Event event, final Map<String, Object> before, final Map<String, Object> after) {

        return before != null && matches(subscription, schema, event, before)
                || after != null && matches(subscription, schema, event, after);
    }

    private boolean matches(final Subscription subscription, final LinkableSchema schema, final Change.Event event, final Map<String, Object> value) {

        return subscription.getSchema().equals(schema.getQualifiedName())
                && subscription.getEvents().contains(event)
                && subscription.getExpression().evaluatePredicate(Context.init(value));
    }
}

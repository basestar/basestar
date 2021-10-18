package io.basestar.stream.hazelcast;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import io.basestar.auth.Caller;
import io.basestar.expression.Expression;
import io.basestar.schema.LinkableSchema;
import io.basestar.stream.*;
import io.basestar.util.Immutable;
import io.basestar.util.Page;
import io.basestar.util.Pager;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public class HazelcastSubscriptions implements Subscriptions {

    private final IMap<SubscriptionKey, Subscription> subscriptions;

    private final Object lock = new Object();

    public HazelcastSubscriptions(final HazelcastInstance instance, final String mapName) {

        subscriptions = instance.getMap(mapName);
    }

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
        return subscriptions.putAsync(new SubscriptionKey(sub, channel), subscription).toCompletableFuture();
    }

    @Override
    public Pager<Subscription> query(final LinkableSchema schema, final Change.Event event, final Map<String, Object> before, final Map<String, Object> after) {

        final Collection<Subscription> matched = subscriptions.values(new SubscriptionPredicate(schema.getQualifiedName(), event, before, after));
        return (stats, token, count) -> CompletableFuture.completedFuture(Page.from(Immutable.list(matched)));
    }

    @Override
    public CompletableFuture<?> unsubscribe(final String sub, final String channel) {

        return subscriptions.removeAsync(new SubscriptionKey(sub, channel)).toCompletableFuture();
    }

    @Override
    public CompletableFuture<?> unsubscribeAll(final String sub) {

        subscriptions.removeAll(new SubscriptionSubPredicate(sub));
        return CompletableFuture.completedFuture(null);
    }
}

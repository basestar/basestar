package io.basestar.stream;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import io.basestar.auth.Caller;
import io.basestar.expression.Expression;
import io.basestar.util.Page;
import io.basestar.util.Pager;

import java.util.*;
import java.util.concurrent.CompletableFuture;

public class MemorySubscriptions implements Subscriptions {

    private final Map<Subscription.Id, Subscription> subscriptions = new HashMap<>();

    private final Multimap<Subscription.Key, Subscription.Id> keyToId = HashMultimap.create();

    private final Multimap<Subscription.Id, Subscription.Key> idToKey = HashMultimap.create();

    private final Object lock = new Object();

    @Override
    public CompletableFuture<?> subscribe(final Caller caller, final String sub, final String channel, final Set<Subscription.Key> keys, final Expression expression, final SubscriptionInfo info) {

        final Subscription subscription = new Subscription();
        subscription.setCaller(caller);
        subscription.setSub(sub);
        subscription.setChannel(channel);
        subscription.setExpression(expression);
        subscription.setInfo(info);
        synchronized (lock) {
            final Subscription.Id id = new Subscription.Id(sub, channel);
            subscriptions.put(id, subscription);
            keys.forEach(key -> {
                keyToId.put(key, id);
                idToKey.put(id, key);
            });
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public List<Pager.Source<Subscription>> query(final Set<Subscription.Key> keys) {

        final List<Subscription> results = new ArrayList<>();
        synchronized (lock) {
            final Set<Subscription.Id> ids = new HashSet<>();
            keys.forEach(key -> {
                ids.addAll(keyToId.get(key));
            });
            ids.forEach(id -> results.add(subscriptions.get(id)));
        }
        return Collections.singletonList((count, token, stats) -> CompletableFuture.completedFuture(Page.from(results)));
    }

    @Override
    public CompletableFuture<?> unsubscribe(final String sub, final String channel) {

        synchronized (lock) {
            final Subscription.Id id = new Subscription.Id(sub, channel);
            idToKey.get(id).forEach(key -> {
                keyToId.remove(key, id);
            });
            idToKey.removeAll(id);
            subscriptions.remove(id);
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<?> unsubscribeAll(final String sub) {

        synchronized (lock) {
            final Set<Subscription.Id> ids = new HashSet<>();
            subscriptions.forEach((id, subscription) -> {
                if(sub.equals(subscription.getSub())) {
                    ids.add(id);
                }
            });
            ids.forEach(id -> {
                idToKey.get(id).forEach(key -> {
                    keyToId.remove(key, id);
                });
                idToKey.removeAll(id);
                subscriptions.remove(id);
            });
        }
        return CompletableFuture.completedFuture(null);
    }
}

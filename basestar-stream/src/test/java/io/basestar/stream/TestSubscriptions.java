package io.basestar.stream;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.basestar.auth.Caller;
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.schema.ObjectSchema;
import io.basestar.schema.Reserved;
import io.basestar.schema.use.UseBinary;
import io.basestar.util.Name;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

abstract class TestSubscriptions {

    protected abstract Subscriptions subscriber();

    @Test
    void testSubscriber() {

        final Subscriptions subscriptions = subscriber();

        final Set<Subscription.Key> keys = ImmutableSet.of(new Subscription.Key(Name.of("Test"), Reserved.PREFIX + ObjectSchema.ID, UseBinary.binaryKey(ImmutableList.of("id"))));
        final SubscriptionInfo info = new TestSubscriptionInfo();

        final Expression expression = Expression.parseAndBind(Context.init(), "id == 'x'");

        final Caller caller1 = Caller.builder().setSuper(true).build();
        final Caller caller2 = Caller.builder().setSuper(true).build();

        subscriptions.subscribe(caller1, "sub", "channel1", keys, expression, info).join();
        subscriptions.subscribe(caller2, "sub", "channel2", keys, expression, info).join();

        final List<Subscription> results = query(subscriptions, keys);
        assertEquals(2, results.size());
        assertTrue(results.contains(new Subscription("sub", "channel1", caller1, expression, info)));
        assertTrue(results.contains(new Subscription("sub", "channel2", caller2, expression, info)));

        subscriptions.unsubscribe("sub", "channel1").join();
        assertEquals(1, query(subscriptions, keys).size());

        subscriptions.unsubscribeAll("sub").join();
        assertEquals(0, query(subscriptions, keys).size());
    }

    private List<Subscription> query(final Subscriptions subscriptions, final Set<Subscription.Key> keys) {

        return subscriptions.query(keys).page(10).join();
    }
}

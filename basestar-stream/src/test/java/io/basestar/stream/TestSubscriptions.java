package io.basestar.stream;

import com.google.common.collect.ImmutableMap;
import io.basestar.auth.Caller;
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.schema.LinkableSchema;
import io.basestar.schema.Namespace;
import io.basestar.schema.ObjectSchema;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public abstract class TestSubscriptions {

    protected abstract Subscriptions subscriber();

    @Test
    void testSubscriber() throws IOException {

        final Namespace namespace = Namespace.load(TestSubscriptions.class.getResource("/schema/Petstore.yml"));

        final ObjectSchema schema = namespace.requireObjectSchema("Cat");

        final Subscriptions subscriptions = subscriber();

        final SubscriptionMetadata info = new TestSubscriptionMetadata();

        final Expression expression = Expression.parseAndBind(Context.init(), "id == 'x'");

        final Caller caller1 = Caller.builder().setSuper(true).build();
        final Caller caller2 = Caller.builder().setSuper(true).build();

        final Set<Change.Event> changes = EnumSet.allOf(Change.Event.class);

        subscriptions.subscribe(caller1, "sub", "channel1", schema, changes, expression, info).join();
        subscriptions.subscribe(caller2, "sub", "channel2", schema, changes, expression, info).join();

        final List<Subscription> results = query(subscriptions, schema, Change.Event.CREATE, null, ImmutableMap.of("id", "x"));
        assertEquals(2, results.size());
        assertTrue(results.contains(new Subscription("sub", "channel1", caller1, schema.getQualifiedName(), changes, expression, info)));
        assertTrue(results.contains(new Subscription("sub", "channel2", caller2, schema.getQualifiedName(), changes, expression, info)));

        subscriptions.unsubscribe("sub", "channel1").join();
        assertEquals(1, query(subscriptions, schema, Change.Event.CREATE, null, ImmutableMap.of("id", "x")).size());

        subscriptions.unsubscribeAll("sub").join();
        assertEquals(0, query(subscriptions, schema, Change.Event.CREATE, null, ImmutableMap.of("id", "x")).size());
    }

    private List<Subscription> query(final Subscriptions subscriptions, final LinkableSchema schema, final Change.Event event, final Map<String, Object> before, final Map<String, Object> after) {

        return subscriptions.query(schema, event, before, after).page(10).join();
    }
}

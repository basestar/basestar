package io.basestar.stream;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.basestar.auth.Caller;
import io.basestar.database.event.ObjectCreatedEvent;
import io.basestar.database.event.ObjectDeletedEvent;
import io.basestar.database.event.ObjectRefreshedEvent;
import io.basestar.database.event.ObjectUpdatedEvent;
import io.basestar.event.Emitter;
import io.basestar.event.Event;
import io.basestar.expression.Expression;
import io.basestar.schema.Namespace;
import io.basestar.schema.ObjectSchema;
import io.basestar.stream.event.SubscriptionPublishEvent;
import io.basestar.stream.event.SubscriptionQueryEvent;
import io.basestar.util.Name;
import io.basestar.util.Pager;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.EnumSet;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TestDefaultHub {

    @Test
    void testEvents() throws IOException {

        final Namespace namespace = Namespace.load(TestSubscriptions.class.getResource("/schema/Petstore.yml"));

        final SubscriptionMetadata meta = new MetadataImpl();

        final Subscription sub = new Subscription();
        sub.setCaller(Caller.ANON);
        sub.setSub("sub1");
        sub.setChannel("channel1");
        sub.setEvents(EnumSet.allOf(Change.Event.class));
        sub.setSchema(Name.of("Cat"));
        sub.setExpression(Expression.parse("name == 'Pippa'"));
        sub.setMetadata(meta);

        final Subscriptions subscriptions = Mockito.mock(Subscriptions.class);
        final Emitter emitter = Mockito.mock(Emitter.class);

        Mockito.when(emitter.emit(Mockito.any(Event.class)))
                .thenReturn(CompletableFuture.completedFuture(null));

        Mockito.when(emitter.emit(Mockito.anyCollectionOf(Event.class)))
                .thenReturn(CompletableFuture.completedFuture(null));

        Mockito.when(subscriptions.query(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any()))
                .thenReturn(Pager.simple(ImmutableList.of(sub)));

        final DefaultHub hub = new DefaultHub(subscriptions, namespace, emitter);

        hub.handle(ObjectCreatedEvent.of(Name.of("Cat"), "1", ImmutableMap.of("name", "Pippa")), ImmutableMap.of()).join();
        Mockito.verify(emitter).emit(SubscriptionQueryEvent.of(Name.of("Cat"), Change.Event.CREATE, null, ImmutableMap.of("name", "Pippa")));

        hub.handle(ObjectUpdatedEvent.of(Name.of("Cat"), "1", 1L, ImmutableMap.of("name", "Pippa"), ImmutableMap.of("name", "Garfield")), ImmutableMap.of()).join();
        Mockito.verify(emitter).emit(SubscriptionQueryEvent.of(Name.of("Cat"), Change.Event.UPDATE, ImmutableMap.of("name", "Pippa"), ImmutableMap.of("name", "Garfield")));

        hub.handle(ObjectRefreshedEvent.of(Name.of("Cat"), "1", 1L, ImmutableMap.of("name", "Garfield"), ImmutableMap.of("name", "Garfield")), ImmutableMap.of()).join();
        Mockito.verify(emitter).emit(SubscriptionQueryEvent.of(Name.of("Cat"), Change.Event.REFRESH, ImmutableMap.of("name", "Garfield"), ImmutableMap.of("name", "Garfield")));

        hub.handle(ObjectDeletedEvent.of(Name.of("Cat"), "1", 1L, ImmutableMap.of("name", "Garfield")), ImmutableMap.of()).join();
        Mockito.verify(emitter).emit(SubscriptionQueryEvent.of(Name.of("Cat"), Change.Event.DELETE, ImmutableMap.of("name", "Garfield"), null));

        hub.handle(SubscriptionQueryEvent.of(Name.of("Cat"), Change.Event.CREATE, ImmutableMap.of("name", "Pippa"), null), ImmutableMap.of()).join();
        Mockito.verify(emitter).emit(ImmutableList.of(SubscriptionPublishEvent.of(Caller.ANON, "sub1", "channel1", Change.of(Change.Event.CREATE, Name.of("Cat"), ImmutableMap.of("name", "Pippa"), null), meta)));
    }

    @Test
    void testSubscribeUnsubscribe() throws IOException {

        final Namespace namespace = Namespace.load(TestSubscriptions.class.getResource("/schema/Petstore.yml"));

        final ObjectSchema schema = namespace.requireObjectSchema("Cat");

        final SubscriptionMetadata meta = new MetadataImpl();

        final Subscriptions subscriptions = new MemorySubscriptions();
        final Emitter emitter = Mockito.mock(Emitter.class);

        final DefaultHub hub = new DefaultHub(subscriptions, namespace, emitter);

        final Expression expression = Expression.parse("name == 'Pippa'");

        hub.subscribe(Caller.ANON, "sub1", "channel1", Name.of("Cat"), expression, meta).join();
        hub.subscribe(Caller.ANON, "sub1", "channel2", Name.of("Cat"), expression, meta).join();
        hub.subscribe(Caller.ANON, "sub1", "channel3", Name.of("Cat"), expression, meta).join();
        assertEquals(3, subscriptions.query(schema, Change.Event.CREATE, ImmutableMap.of("name", "Pippa"), null).page(1).join().size());

        hub.unsubscribe(Caller.ANON, "sub1", "channel1");
        assertEquals(2, subscriptions.query(schema, Change.Event.CREATE, ImmutableMap.of("name", "Pippa"), null).page(1).join().size());

        hub.unsubscribeAll(Caller.ANON, "sub1");
        assertEquals(0, subscriptions.query(schema, Change.Event.CREATE, ImmutableMap.of("name", "Pippa"), null).page(1).join().size());
    }

    private static class MetadataImpl implements SubscriptionMetadata {

    }
}

package io.basestar.stream;

/*-
 * #%L
 * basestar-stream
 * %%
 * Copyright (C) 2019 - 2020 Basestar.IO
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import io.basestar.auth.Caller;
import io.basestar.database.event.ObjectCreatedEvent;
import io.basestar.database.event.ObjectDeletedEvent;
import io.basestar.database.event.ObjectRefreshedEvent;
import io.basestar.database.event.ObjectUpdatedEvent;
import io.basestar.event.Emitter;
import io.basestar.event.Event;
import io.basestar.event.Handlers;
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.schema.LinkableSchema;
import io.basestar.schema.Namespace;
import io.basestar.stream.event.SubscriptionPublishEvent;
import io.basestar.stream.event.SubscriptionQueryEvent;
import io.basestar.util.Name;
import io.basestar.util.Nullsafe;
import io.basestar.util.Pager;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public class DefaultHub implements Hub {

    private static final int SUBSCRIPTION_PAGE_SIZE = 50;

    private static final Handlers<DefaultHub> HANDLERS = Handlers.<DefaultHub>builder()
            .on(ObjectCreatedEvent.class, DefaultHub::onObjectCreated)
            .on(ObjectUpdatedEvent.class, DefaultHub::onObjectUpdated)
            .on(ObjectDeletedEvent.class, DefaultHub::onObjectDeleted)
            .on(ObjectRefreshedEvent.class, DefaultHub::onObjectRefreshed)
            .on(SubscriptionQueryEvent.class, DefaultHub::onSubscriptionQuery)
            .build();

    private final Subscriptions subscriptions;

    private final Namespace namespace;

    private final Emitter emitter;

    @lombok.Builder(builderClassName = "Builder")
    DefaultHub(final Subscriptions subscriptions, final Namespace namespace, final Emitter emitter) {

        this.subscriptions = Nullsafe.require(subscriptions);
        this.namespace = Nullsafe.require(namespace);
        this.emitter = Nullsafe.require(emitter);
    }

    @Override
    public CompletableFuture<?> handle(final Event event, final Map<String, String> meta) {

        return HANDLERS.handle(this, event, meta);
    }

    @Override
    public CompletableFuture<?> subscribe(final Caller caller, final String sub, final String channel, final Name schemaName, final Set<Change.Event> events, final Expression expression, final SubscriptionMetadata info) {

        final LinkableSchema schema = namespace.requireLinkableSchema(schemaName);

        final Expression bound = expression.bind(Context.init());
        return subscriptions.subscribe(caller, sub, channel, schema, events, bound, info);
    }

    @Override
    public CompletableFuture<?> unsubscribe(final Caller caller, final String sub, final String channel) {

        return subscriptions.unsubscribe(sub, channel);
    }

    @Override
    public CompletableFuture<?> unsubscribeAll(final Caller caller, final String sub) {

        return subscriptions.unsubscribeAll(sub);
    }

    private CompletableFuture<?> onObjectCreated(final ObjectCreatedEvent event) {

        final LinkableSchema schema = namespace.requireLinkableSchema(event.getSchema());
        final Map<String, Object> after = event.getAfter();
        return emitter.emit(SubscriptionQueryEvent.of(schema.getQualifiedName(), Change.Event.CREATE, null, after));
    }

    private CompletableFuture<?> onObjectUpdated(final ObjectUpdatedEvent event) {

        final LinkableSchema schema = namespace.requireLinkableSchema(event.getSchema());
        final Map<String, Object> before = event.getBefore();
        final Map<String, Object> after = event.getAfter();
        return emitter.emit(SubscriptionQueryEvent.of(schema.getQualifiedName(), Change.Event.UPDATE, before, after));
    }

    private CompletableFuture<?> onObjectDeleted(final ObjectDeletedEvent event) {

        final LinkableSchema schema = namespace.requireLinkableSchema(event.getSchema());
        final Map<String, Object> before = event.getBefore();
        return emitter.emit(SubscriptionQueryEvent.of(schema.getQualifiedName(), Change.Event.DELETE, before, null));
    }

    private CompletableFuture<?> onObjectRefreshed(final ObjectRefreshedEvent event) {

        final LinkableSchema schema = namespace.requireLinkableSchema(event.getSchema());
        final Map<String, Object> before = event.getBefore();
        final Map<String, Object> after = event.getAfter();
        return emitter.emit(SubscriptionQueryEvent.of(schema.getQualifiedName(), Change.Event.REFRESH, before, after));
    }

    private CompletableFuture<?> onSubscriptionQuery(final SubscriptionQueryEvent event) {

        final LinkableSchema schema = namespace.requireLinkableSchema(event.getSchema());
        final Pager<Subscription> pager = subscriptions.query(schema, event.getEvent(), event.getBefore(), event.getAfter());
        return pager.page(event.getPaging(), SUBSCRIPTION_PAGE_SIZE).thenCompose(results -> {
            final List<Event> events = new ArrayList<>();

            results.forEach(result -> {
                final Caller caller = result.getCaller();
                final Map<String, Object> before = event.getBefore();
                final Map<String, Object> after = event.getAfter();
                final SubscriptionMetadata metadata = result.getMetadata();
                if(match(before, result.getExpression()) || match(after, result.getExpression())) {
                    events.add(SubscriptionPublishEvent.of(caller, result.getSub(), result.getChannel(), Change.of(event.getEvent(), schema.getQualifiedName(),  before, after), metadata));
                }
            });

            if(results.hasMore()) {
                events.add(event.withPaging(results.getPaging()));
            }
            return emitter.emit(events);
        });
    }

    private boolean match(final Map<String, Object> object, final Expression expression) {

        if(object != null) {
            return expression.evaluatePredicate(Context.init(object));
        } else {
            return false;
        }
    }
}

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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
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
import io.basestar.schema.*;
import io.basestar.storage.PartitionedStorage;
import io.basestar.storage.exception.UnsupportedQueryException;
import io.basestar.storage.query.DisjunctionVisitor;
import io.basestar.storage.query.Range;
import io.basestar.storage.query.RangeVisitor;
import io.basestar.stream.event.SubscriptionPublishEvent;
import io.basestar.stream.event.SubscriptionQueryEvent;
import io.basestar.util.Name;
import io.basestar.util.Nullsafe;
import io.basestar.util.Pager;

import java.util.*;
import java.util.concurrent.CompletableFuture;

public class DefaultHub implements Hub {

    private static final int SUBSCRIPTION_PAGE_SIZE = 50;

    private static final Handlers<DefaultHub> HANDLERS = Handlers.<DefaultHub>builder()
            .on(ObjectCreatedEvent.class, DefaultHub::onObjectCreated)
            .on(ObjectUpdatedEvent.class, DefaultHub::onObjectUpdated)
            .on(ObjectDeletedEvent.class, DefaultHub::onObjectDeleted)
            .on(ObjectRefreshedEvent.class, DefaultHub::onObjectRefreshed)
            .on(SubscriptionQueryEvent.class, DefaultHub::onSubscriptionQuery)
            .on(SubscriptionPublishEvent.class, DefaultHub::onSubscriptionPublish)
            .build();

    private final Subscriptions subscriptions;

    private final Publisher publisher;

    private final Namespace namespace;

    private final Emitter emitter;

    @lombok.Builder(builderClassName = "Builder")
    DefaultHub(final Subscriptions subscriptions, final Publisher publisher,
               final Namespace namespace, final Emitter emitter) {

        this.subscriptions = Nullsafe.require(subscriptions);
        this.publisher = Nullsafe.require(publisher);
        this.namespace = Nullsafe.require(namespace);
        this.emitter = Nullsafe.require(emitter);
    }

    @Override
    public CompletableFuture<?> handle(final Event event, final Map<String, String> meta) {

        return HANDLERS.handle(this, event, meta);
    }

    @Override
    public CompletableFuture<?> subscribe(final Caller caller, final String sub, final String channel, final String schemaName, final Expression expression, final Set<Name> expand, final SubscriptionInfo info) {

        final ObjectSchema schema = namespace.requireObjectSchema(schemaName);

        final Expression bound = expression.bind(Context.init());
        final Set<Subscription.Key> keys = keys(schema, expression, expand);
        return subscriptions.subscribe(caller, sub, channel, keys, bound, info);
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

        final ObjectSchema schema = namespace.requireObjectSchema(event.getSchema());
        final Map<String, Object> after = event.getAfter();
        final Set<Subscription.Key> keys = keys(schema, after);
        return emitter.emit(SubscriptionQueryEvent.of(schema.getQualifiedName(), event.getId(), Change.Event.CREATE, null, after, keys));
    }

    private CompletableFuture<?> onObjectUpdated(final ObjectUpdatedEvent event) {

        final ObjectSchema schema = namespace.requireObjectSchema(event.getSchema());
        final Map<String, Object> before = event.getBefore();
        final Map<String, Object> after = event.getAfter();
        final Set<Subscription.Key> keys = Sets.union(keys(schema, after), keys(schema, before));
        return emitter.emit(SubscriptionQueryEvent.of(schema.getQualifiedName(), event.getId(), Change.Event.UPDATE, before, after, keys));
    }

    private CompletableFuture<?> onObjectDeleted(final ObjectDeletedEvent event) {

        final ObjectSchema schema = namespace.requireObjectSchema(event.getSchema());
        final Map<String, Object> before = event.getBefore();
        final Set<Subscription.Key> keys = keys(schema, before);
        return emitter.emit(SubscriptionQueryEvent.of(schema.getQualifiedName(), event.getId(), Change.Event.DELETE, before, null, keys));
    }

    private CompletableFuture<?> onObjectRefreshed(final ObjectRefreshedEvent event) {

        final ObjectSchema schema = namespace.requireObjectSchema(event.getSchema());
        final Map<String, Object> before = event.getBefore();
        final Map<String, Object> after = event.getAfter();
        final Set<Subscription.Key> keys = Sets.union(keys(schema, after), keys(schema, before));
        return emitter.emit(SubscriptionQueryEvent.of(schema.getQualifiedName(), event.getId(), Change.Event.REFRESH, before, null, keys));
    }

    private CompletableFuture<?> onSubscriptionQuery(final SubscriptionQueryEvent event) {

        final ObjectSchema schema = namespace.requireObjectSchema(event.getSchema());
        final Set<Subscription.Key> keys = event.getKeys();
        final Comparator<Subscription> comparator = Subscription.COMPARATOR;
        final Pager<Subscription> pager = new Pager<>(comparator, subscriptions.query(keys), event.getPaging());
        return pager.page(SUBSCRIPTION_PAGE_SIZE).thenCompose(results -> {
            final List<Event> events = new ArrayList<>();

            results.forEach(result -> {
                events.add(SubscriptionPublishEvent.of(schema.getQualifiedName(), event.getId(), event.getEvent(), event.getBefore(), event.getAfter(), result));
            });

            if(results.hasMore()) {
                events.add(event.withPaging(results.getPaging()));
            }
            return emitter.emit(events);
        });
    }

    private CompletableFuture<?> onSubscriptionPublish(final SubscriptionPublishEvent event) {

        final ObjectSchema schema = namespace.requireObjectSchema(event.getSchema());
        final String id = event.getId();
        final Subscription subscription = event.getSubscription();
        //FIXME:
        final Caller caller = subscription.getCaller();
        final Expression expression = subscription.getExpression();
        final Instance before = event.getBefore() == null ? null : schema.create(event.getBefore());
        final Instance after = event.getAfter() == null ? null : schema.create(event.getAfter());
        if(match(before, expression) || match(after, expression)) {
            final Change change = Change.of(event.getEvent(), schema.getQualifiedName(), id, before, after);
            return publisher.publish(caller, schema, subscription.getSub(), subscription.getChannel(), subscription.getInfo(), change);
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }

    private boolean match(final Map<String, Object> object, final Expression expression) {

        if(object != null) {
            return expression.evaluatePredicate(Context.init(object));
        } else {
            return false;
        }
    }

    private static Subscription.Key idKey(final ObjectSchema schema, final String id) {

        return new Subscription.Key(schema.getQualifiedName(), Reserved.PREFIX + ObjectSchema.ID, ImmutableList.of(id));
    }

    private static Set<Subscription.Key> keys(final ObjectSchema schema, final Expression expression, final Set<Name> expand) {

        final Set<Expression> disjunction = expression.visit(new DisjunctionVisitor());

        final Set<Subscription.Key> keys = new HashSet<>();
        for (final Expression conjunction : disjunction) {
            final Map<Name, Range<Object>> query = conjunction.visit(new RangeVisitor());
            final Optional<String> optId = PartitionedStorage.constantId(query);
            if(optId.isPresent()) {
                keys.add(idKey(schema, optId.get()));
            } else {
                final Optional<PartitionedStorage.SatisfyResult> optSatisfy = PartitionedStorage.satisfy(schema.getIndexes().values(), query, Collections.emptyList());
                if (optSatisfy.isPresent()) {
                    final PartitionedStorage.SatisfyResult satisfy = optSatisfy.get();
                    final Index index = satisfy.getIndex();
                    keys.add(new Subscription.Key(schema.getQualifiedName(), index.getName(), satisfy.getPartition()));
                } else {
                    throw new UnsupportedQueryException(schema.getQualifiedName(), expression, "no index");
                }
            }
        }
        return keys;
    }

    private static Set<Subscription.Key> keys(final ObjectSchema schema, final Map<String, Object> object) {

        final Set<Subscription.Key> keys = new HashSet<>();
        keys.add(idKey(schema, Instance.getId(object)));
        for(final Index index : schema.getIndexes().values()) {
            index.readValues(object).forEach((k, projection) -> {
                keys.add(new Subscription.Key(schema.getQualifiedName(), index.getName(), k.getPartition()));
            });
        }
        return keys;
    }
}

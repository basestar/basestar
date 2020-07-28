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
import io.basestar.database.Database;
import io.basestar.database.event.ObjectCreatedEvent;
import io.basestar.database.event.ObjectDeletedEvent;
import io.basestar.database.event.ObjectUpdatedEvent;
import io.basestar.database.options.ReadOptions;
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
import io.basestar.storage.util.Pager;
import io.basestar.stream.event.SubscriptionPublishEvent;
import io.basestar.stream.event.SubscriptionQueryEvent;
import io.basestar.util.Name;

import java.util.*;
import java.util.concurrent.CompletableFuture;

public class DefaultBroadcaster implements Broadcaster {

    private static final int SUBSCRIPTION_PAGE_SIZE = 50;

    private static final Handlers<DefaultBroadcaster> HANDLERS = Handlers.<DefaultBroadcaster>builder()
            .on(ObjectCreatedEvent.class, DefaultBroadcaster::onObjectCreated)
            .on(ObjectUpdatedEvent.class, DefaultBroadcaster::onObjectUpdated)
            .on(ObjectDeletedEvent.class, DefaultBroadcaster::onObjectDeleted)
            .on(SubscriptionQueryEvent.class, DefaultBroadcaster::onSubscriptionQuery)
            .on(SubscriptionPublishEvent.class, DefaultBroadcaster::onSubscriptionPublish)
            .build();

    private final Subscriber subscriber;

    private final Publisher publisher;

    private final Database database;

    private final Namespace namespace;

    private final Emitter emitter;

    @lombok.Builder(builderClassName = "Builder")
    DefaultBroadcaster(final Subscriber subscriber, final Publisher publisher,
                       final Database database, final Namespace namespace,
                       final Emitter emitter) {

        this.subscriber = subscriber;
        this.publisher = publisher;
        this.database = database;
        this.namespace = namespace;
        this.emitter = emitter;
    }

    @Override
    public CompletableFuture<?> handle(final Event event, final Map<String, String> meta) {

        return HANDLERS.handle(this, event, meta);
    }

    @Override
    public CompletableFuture<?> subscribe(final Caller caller, final String sub, final String channel, final String schemaName, final Expression expression, final Set<Name> expand) {

        final ObjectSchema schema = namespace.requireObjectSchema(schemaName);

        final Expression bound = expression.bind(Context.init());
        final Set<Subscription.Key> keys = keys(schema, expression);
        return subscriber.subscribe(caller, sub, channel, keys, bound, expand);
    }

    @Override
    public CompletableFuture<?> unsubscribe(final Caller caller, final String sub, final String channel) {

        if(channel == null) {
            return subscriber.unsubscribeAll(sub);
        } else {
            return subscriber.unsubscribe(sub, channel);
        }
    }

    private CompletableFuture<?> onObjectCreated(final ObjectCreatedEvent event) {

        final ObjectSchema schema = namespace.requireObjectSchema(event.getSchema());
        final Map<String, Object> after = event.getAfter();
        final Set<Subscription.Key> keys = keys(schema, after);
        return emitter.emit(SubscriptionQueryEvent.of(schema.getQualifiedName(), event.getId(), Change.Event.CREATE, null, Instance.getVersion(after), keys));
    }

    private CompletableFuture<?> onObjectUpdated(final ObjectUpdatedEvent event) {

        final ObjectSchema schema = namespace.requireObjectSchema(event.getSchema());
        final Map<String, Object> before = event.getBefore();
        final Map<String, Object> after = event.getAfter();
        final Set<Subscription.Key> keys = Sets.union(keys(schema, after), keys(schema, before));
        return emitter.emit(SubscriptionQueryEvent.of(schema.getQualifiedName(), event.getId(), Change.Event.UPDATE, Instance.getVersion(before), Instance.getVersion(after), keys));
    }

    private CompletableFuture<?> onObjectDeleted(final ObjectDeletedEvent event) {

        final ObjectSchema schema = namespace.requireObjectSchema(event.getSchema());
        final Map<String, Object> before = event.getBefore();
        final Set<Subscription.Key> keys = keys(schema, before);
        return emitter.emit(SubscriptionQueryEvent.of(schema.getQualifiedName(), event.getId(), Change.Event.DELETE, Instance.getVersion(before), null, keys));
    }

    private CompletableFuture<?> onSubscriptionQuery(final SubscriptionQueryEvent event) {

        final ObjectSchema schema = namespace.requireObjectSchema(event.getSchema());
        final Set<Subscription.Key> keys = event.getKeys();
        final Comparator<Subscription> comparator = Subscription.COMPARATOR;
        final Pager<Subscription> pager = new Pager<>(comparator, subscriber.query(keys), event.getPaging());
        return pager.page(SUBSCRIPTION_PAGE_SIZE).thenCompose(results -> {
            final List<Event> events = new ArrayList<>();

            results.forEach(result -> {
                events.add(SubscriptionPublishEvent.of(schema.getQualifiedName(), event.getId(), event.getEvent(), event.getBefore(), event.getAfter(), result));
            });

            if(results.hasPaging()) {
                events.add(event.withPaging(results.getPaging()));
            }
            return emitter.emit(events);
        });
    }

    private CompletableFuture<?> onSubscriptionPublish(final SubscriptionPublishEvent event) {

        final ObjectSchema schema = namespace.requireObjectSchema(event.getSchema());
        final String id = event.getId();
        final Subscription subscription = event.getSubscription();
        final Caller caller = subscription.getCaller();
        final Expression expression = subscription.getExpression();
        final Set<Name> expand = subscription.getExpand();
        final CompletableFuture<Instance> beforeFuture = load(caller, schema, id, event.getBefore(), expand);
        final CompletableFuture<Instance> afterFuture = load(caller, schema, id, event.getAfter(), expand);
        return CompletableFuture.allOf(beforeFuture, afterFuture).thenCompose(ignored -> {
            final Instance before = beforeFuture.getNow(null);
            final Instance after = afterFuture.getNow(null);
            if(match(before, expression) || match(after, expression)) {
                return publisher.publish(subscription.getSub(), subscription.getChannel(), Change.of(event.getEvent(), schema.getQualifiedName(), id, before, after));
            } else {
                return CompletableFuture.completedFuture(null);
            }
        });
    }

    private boolean match(final Map<String, Object> object, final Expression expression) {

        if(object != null) {
            return expression.evaluatePredicate(Context.init(object));
        } else {
            return false;
        }
    }

    private CompletableFuture<Instance> load(final Caller caller, final ObjectSchema schema, final String id, final Long version, final Set<Name> expand) {

        if(version != null) {
            return database.read(caller, ReadOptions.builder().schema(schema.getQualifiedName()).id(id).version(version).expand(expand).build());
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }

    private static Subscription.Key idKey(final ObjectSchema schema, final String id) {

        return new Subscription.Key(schema.getQualifiedName(), Reserved.PREFIX + Reserved.ID, ImmutableList.of(id));
    }

    private static Set<Subscription.Key> keys(final ObjectSchema schema, final Expression expression) {

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

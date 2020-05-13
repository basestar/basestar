package io.basestar.database;

/*-
 * #%L
 * basestar-database-server
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

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import io.basestar.auth.Caller;
import io.basestar.auth.exception.PermissionDeniedException;
import io.basestar.database.action.Action;
import io.basestar.database.action.CreateAction;
import io.basestar.database.action.DeleteAction;
import io.basestar.database.action.UpdateAction;
import io.basestar.database.event.*;
import io.basestar.database.exception.BatchKeyRepeatedException;
import io.basestar.database.options.*;
import io.basestar.database.util.ExpandKey;
import io.basestar.database.util.RefKey;
import io.basestar.event.Emitter;
import io.basestar.event.Event;
import io.basestar.event.Handler;
import io.basestar.event.Handlers;
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.expression.PathTransform;
import io.basestar.expression.constant.Constant;
import io.basestar.expression.logical.And;
import io.basestar.expression.logical.Or;
import io.basestar.schema.*;
import io.basestar.storage.OverlayStorage;
import io.basestar.storage.Storage;
import io.basestar.storage.StorageTraits;
import io.basestar.storage.exception.ObjectMissingException;
import io.basestar.storage.util.IndexRecordDiff;
import io.basestar.util.*;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class DatabaseServer extends ReadProcessor implements Database, Handler<Event>, CommonVars {

    private static final String SINGLE_BATCH_ROOT = "$";

    private static final int REF_QUERY_BATCH_SIZE = 100;

    private final Emitter emitter;

    private static final Handlers<DatabaseServer> HANDLERS = Handlers.<DatabaseServer>builder()
            .on(ObjectCreatedEvent.class, DatabaseServer::onObjectCreated)
            .on(ObjectUpdatedEvent.class, DatabaseServer::onObjectUpdated)
            .on(ObjectDeletedEvent.class, DatabaseServer::onObjectDeleted)
            .on(ObjectRefreshedEvent.class, DatabaseServer::onObjectRefreshed)
            .on(AsyncIndexCreatedEvent.class, DatabaseServer::onAsyncIndexCreated)
            .on(AsyncIndexUpdatedEvent.class, DatabaseServer::onAsyncIndexUpdated)
            .on(AsyncIndexDeletedEvent.class, DatabaseServer::onAsyncIndexDeleted)
            .on(AsyncIndexDeletedEvent.class, DatabaseServer::onAsyncIndexDeleted)
            .on(AsyncHistoryCreatedEvent.class, DatabaseServer::onAsyncHistoryCreated)
            .on(RefQueryEvent.class, DatabaseServer::onRefQuery)
            .on(RefRefreshEvent.class, DatabaseServer::onRefRefresh)
            .build();

    public DatabaseServer(final Namespace namespace, final Storage storage) {

        this(namespace, storage, Emitter.skip());
    }

    public DatabaseServer(final Namespace namespace, final Storage storage, final Emitter emitter) {

        super(namespace, storage);
        this.emitter = emitter;
    }

    @Override
    public CompletableFuture<?> handle(final Event event) {

        return HANDLERS.handle(this, event);
    }

    @Override
    public Namespace namespace() {

        return namespace;
    }

    @Override
    public CompletableFuture<Map<String, Instance>> transaction(final Caller caller, final TransactionOptions options) {

        log.debug("Batch: options={}", options);

        final Map<String, Action> actions = new HashMap<>();
        options.getActions().forEach((name, action) -> {
            if (action instanceof CreateOptions) {
                final CreateOptions create = (CreateOptions)action;
                actions.put(name, new CreateAction(objectSchema(create.getSchema()), create));
            } else if (action instanceof UpdateOptions) {
                final UpdateOptions update = (UpdateOptions)action;
                actions.put(name, new UpdateAction(objectSchema(update.getSchema()), update));
            } else if (action instanceof DeleteOptions) {
                final DeleteOptions delete = (DeleteOptions)action;
                actions.put(name, new DeleteAction(objectSchema(delete.getSchema()), delete));
            }
        });

        return batch(caller, actions);
    }

    private CompletableFuture<Instance> single(final Caller caller, final Action action) {

        return batch(caller, ImmutableMap.of(SINGLE_BATCH_ROOT, action))
                .thenApply(v -> v.get(SINGLE_BATCH_ROOT));
    }

    private Set<Path> permissionExpand(final ObjectSchema schema, final Permission permission) {

        return permission == null ? Collections.emptySet() : Nullsafe.option(permission.getExpand());
    }

    private CompletableFuture<Map<String, Instance>> batch(final Caller caller, final Map<String, Action> actions) {

        final Set<RefKey> beforeCheck = new HashSet<>();
        final Set<ExpandKey<RefKey>> beforeKeys = new HashSet<>();
        final Set<Path> beforeCallerExpand = new HashSet<>();

        actions.forEach((name, action) -> {
            final String id = action.id();
            if (id != null) {
                final ObjectSchema schema = action.schema();
                final RefKey key = new RefKey(schema.getName(), id);
                if (!beforeCheck.add(key)) {
                    throw new BatchKeyRepeatedException(key.getSchema(), key.getId());
                }
                final Set<Path> permissionExpand = Sets.union(
                        permissionExpand(schema, action.permission()),
                        // Always need read permission for the target object
                        permissionExpand(schema, schema.getPermission(Permission.READ))
                );
                beforeCallerExpand.addAll(Path.children(permissionExpand, Path.of(VAR_CALLER)));
                final Set<Path> readExpand = Path.children(permissionExpand, Path.of(VAR_BEFORE));
                final Set<Path> transientExpand = schema.transientExpand(Path.of(), readExpand);
                beforeKeys.add(ExpandKey.from(key, transientExpand));
            }
        });

        return expandCaller(Context.init(), caller, beforeCallerExpand).thenCompose(beforeCaller -> {

            final Context beforeContext = context(beforeCaller);

            // Read initial values

            final CompletableFuture<Map<RefKey, Instance>> beforeFuture;
            if (!beforeKeys.isEmpty()) {
                final Storage.ReadTransaction read = storage.read(Consistency.NONE);
                beforeKeys.forEach(expandKey -> {
                    final RefKey key = expandKey.getKey();
                    read.readObject(objectSchema(key.getSchema()), key.getId());
                });
                beforeFuture = read.read().thenCompose(readResults -> {

                    final Map<ExpandKey<RefKey>, Instance> beforeUnexpanded = new HashMap<>();
                    beforeKeys.forEach(expandKey -> {
                        final RefKey key = expandKey.getKey();
                        final Map<String, Object> data = readResults.getObject(key.getSchema(), key.getId());
                        if (data != null) {
                            beforeUnexpanded.put(expandKey, objectSchema(key.getSchema()).create(data));
                        }
                    });

                    // FIXME need unexpanded results
                    return expand(beforeContext, beforeUnexpanded)
                            .thenApply(this::processActionResults);
                });
            } else {
                beforeFuture = CompletableFuture.completedFuture(Collections.emptyMap());
            }

            return beforeFuture.thenCompose(beforeResults -> {

                final Set<RefKey> afterCheck = new HashSet<>();
                final Map<ExpandKey<RefKey>, Instance> afterKeys = new HashMap<>();
                final Set<Path> afterCallerExpand = new HashSet<>();

                final Map<String, RefKey> resultLookup = new HashMap<>();
                final Map<String, Instance> overlay = new HashMap<>();

                // Compute changes

                actionOrder(actions).forEach(name-> {
                    final Action action = actions.get(name);
                    final ObjectSchema schema = action.schema();
                    final String id = action.id();
                    final RefKey beforeKey;
                    final Instance before;
                    if (id != null) {
                        beforeKey = new RefKey(schema.getName(), id);
                        before = beforeResults.get(beforeKey);
                    } else {
                        beforeKey = null;
                        before = null;
                    }
                    final Instance after = action.after(beforeContext.with(VAR_BATCH, overlay), before);
                    if (after != null) {
                        final RefKey afterKey = RefKey.from(after);
                        if (!afterCheck.add(afterKey)) {
                            throw new BatchKeyRepeatedException(afterKey.getSchema(), afterKey.getId());
                        }
                        resultLookup.put(name, afterKey);
                        overlay.put(name, after);
                        assert beforeKey == null || beforeKey.equals(afterKey);
                        final Set<Path> permissionExpand = permissionExpand(schema, action.permission());
                        afterCallerExpand.addAll(Path.children(permissionExpand, Path.of(VAR_CALLER)));
                        final Set<Path> readExpand = Sets.union(
                                Path.children(permissionExpand, Path.of(VAR_AFTER)),
                                Nullsafe.option(action.afterExpand())
                        );
                        final Set<Path> transientExpand = schema.transientExpand(Path.of(), readExpand);
                        final Set<Path> writeExpand = Sets.union(transientExpand, schema.getExpand());
                        final ExpandKey<RefKey> expandKey = ExpandKey.from(afterKey, writeExpand);
                        afterKeys.put(expandKey, after);
                    } else {
                        assert beforeKey != null;
                        if (!afterCheck.add(beforeKey)) {
                            throw new BatchKeyRepeatedException(beforeKey.getSchema(), beforeKey.getId());
                        }
                        resultLookup.put(name, beforeKey);
                    }
                });

                // Perform expansion using overlay storage, so that permissions can reference other batch actions

                final Storage overlayStorage = new OverlayStorage(storage, overlay.values());
                final ReadProcessor readOverlay = new ReadProcessor(namespace, overlayStorage);

                return readOverlay.expandCaller(beforeContext, beforeCaller, afterCallerExpand).thenCompose(afterCaller -> {

                    final Context afterContext = context(afterCaller);

                    final CompletableFuture<Map<RefKey, Instance>> afterFuture;
                    if (afterKeys.isEmpty()) {
                        afterFuture = CompletableFuture.completedFuture(Collections.emptyMap());
                    } else {
                        afterFuture = readOverlay.expand(afterContext, afterKeys)
                                .thenApply(this::processActionResults);
                    }

                    return afterFuture.thenCompose(afterResults -> {

                        // Check permissions
                        actions.forEach((name, action) -> {
                            final ObjectSchema schema = action.schema();
                            final RefKey key = resultLookup.get(name);
                            final Permission permission = action.permission();
                            final Instance before = beforeResults.get(key);
                            final Instance after = afterResults.get(key);
                            final Map<String, Object> scope = new HashMap<>();
                            // FIXME: might make sense for before/after to be allowed to be null in scope
                            if(before != null) {
                                scope.put(VAR_BEFORE, before);
                            }
                            if(after != null) {
                                scope.put(VAR_AFTER, after);
                            }
                            checkPermission(afterCaller, schema, permission, scope);
                        });

                        final Storage.WriteTransaction write = storage.write(Consistency.ATOMIC);

                        final Map<String, Instance> results = new HashMap<>();
                        final Set<Event> events = new HashSet<>();

                        // Perform writes
                        actions.forEach((name, action) -> {
                            final ObjectSchema schema = action.schema();
                            final RefKey key = resultLookup.get(name);
                            assert key != null;
                            final Instance before = beforeResults.get(key);
                            final Instance after = afterResults.get(key);
                            if (before == null) {
                                assert after != null;
                                writeCreate(write, schema, key.getId(), after);
                            } else if (after != null) {
                                writeUpdate(write, schema, key.getId(), before, after);
                            } else {
                                writeDelete(write, schema, key.getId(), before);
                            }
                            if (storage.eventStrategy(schema) == Storage.EventStrategy.EMIT) {
                                events.add(action.event(before, after));
                            }
                            // Remove superfluous expand data that was only used for permissions
                            final Instance restricted;
                            if(after == null) {
                                restricted = null;
                            } else {
                                final Instance expanded = schema.expand(after, Expander.noop(), Nullsafe.option(action.afterExpand()));
                                restricted = schema.applyVisibility(afterContext, expanded);
                            }
                            results.put(name, restricted);
                        });

                        return write.commit()
                                .thenCompose(ignored -> emitter.emit(events))
                                .thenApply(ignored -> results);
                    });

                });
            });

        });
    }

    private Map<RefKey, Instance> processActionResults(final Map<ExpandKey<RefKey>, Instance> expanded) {

        final Map<RefKey, Instance> results = new HashMap<>();
        expanded.forEach((k, v) -> results.put(k.getKey(), v));
        return results;
    }

    private List<String> actionOrder(final Map<String, Action> actions) {

        final LinkedHashMap<String, Set<String>> dependencies = new LinkedHashMap<>();
        actions.forEach((name, action) -> {
            final Set<Path> paths = Path.children(action.paths(), Path.of(VAR_BATCH));
            final Set<String> matches = paths.stream().map(AbstractPath::first)
                    .filter(actions::containsKey)
                    .collect(Collectors.toSet());
            dependencies.put(name, matches);
        });

        return TopologicalSort.stableSort(dependencies.keySet(), dependencies::get);
    }

    private void writeCreate(final Storage.WriteTransaction write, final ObjectSchema schema, final String id, final Instance after) {

        write.createObject(schema, id, after);

        objectHierarchy(schema).forEach(superSchema -> {

            final Instance superAfter = superSchema.create(after);
            write.createObject(superSchema, id, superAfter);
        });
    }

    private void writeUpdate(final Storage.WriteTransaction write, final ObjectSchema schema, final String id, final Instance before, final Instance after) {

        write.updateObject(schema, id, before, after);

        objectHierarchy(schema).forEach(superSchema -> {

            final Instance superBefore = superSchema.create(before);
            final Instance superAfter = superSchema.create(after);
            write.updateObject(superSchema, id, superBefore, superAfter);
        });
    }

    private void writeDelete(final Storage.WriteTransaction write, final ObjectSchema schema, final String id, final Instance before) {

        write.deleteObject(schema, id, before);

        objectHierarchy(schema).forEach(superSchema -> {
            final Instance superBefore = superSchema.create(before);
            write.deleteObject(superSchema, id, superBefore);
        });
    }

    private List<ObjectSchema> objectHierarchy(final ObjectSchema schema) {

        final InstanceSchema parent = schema.getExtend();
        if(parent instanceof ObjectSchema) {
            final ObjectSchema objectParent = (ObjectSchema)parent;
            return ImmutableList.<ObjectSchema>builder()
                    .addAll(objectHierarchy(objectParent))
                    .add(objectParent)
                    .build();
        } else {
            return ImmutableList.of();
        }
    }

    @Override
    public CompletableFuture<Instance> read(final Caller caller, final ReadOptions options) {

        log.debug("Read: options={}", options);

        final String id = options.getId();
        final ObjectSchema objectSchema = namespace.requireObjectSchema(options.getSchema());

        return readImpl(objectSchema, id, options.getVersion())
                .thenCompose(initial -> expandAndRestrict(caller, initial, options.getExpand()));
    }

    // FIXME need to apply nested permissions
    private Instance restrict(final Caller caller, final Instance instance, final Set<Path> expand) {

        final ObjectSchema schema = objectSchema(Instance.getSchema(instance));
        final Permission read = schema.getPermission(Permission.READ);
        checkPermission(caller, schema, read, ImmutableMap.of(VAR_THIS, instance));
        final Instance visible = schema.applyVisibility(context(caller), instance);
        return schema.expand(visible, Expander.noop(), expand);
    }

    // FIXME need to create a deeper permission expand for nested permissions
    private CompletableFuture<Instance> expandAndRestrict(final Caller caller, final Instance instance, final Set<Path> expand) {

        if(instance == null) {
            return CompletableFuture.completedFuture(null);
        }

        final ObjectSchema schema = objectSchema(Instance.getSchema(instance));
        final Permission read = schema.getPermission(Permission.READ);
        final Set<Path> permissionExpand = permissionExpand(schema, read);
        final Set<Path> callerExpand = Path.children(permissionExpand, Path.of(VAR_CALLER));
        final Set<Path> readExpand = Sets.union(Path.children(permissionExpand, Path.of(VAR_THIS)), Nullsafe.option(expand));
        final Set<Path> transientExpand = schema.transientExpand(Path.of(), readExpand);

        return expandCaller(Context.init(), caller, callerExpand)
                .thenCompose(expandedCaller -> expand(context(expandedCaller), instance, transientExpand)
                .thenApply(v -> restrict(caller, v, expand)));
    }

    // FIXME need to create a deeper permission expand for nested permissions
    private CompletableFuture<PagedList<Instance>> expandAndRestrict(final Caller caller, final PagedList<Instance> instances, final Set<Path> expand) {

        final Set<Path> callerExpand = new HashSet<>();
        final Set<Path> transientExpand = new HashSet<>();
        for(final Instance instance : instances) {
            final ObjectSchema schema = objectSchema(Instance.getSchema(instance));
            final Permission read = schema.getPermission(Permission.READ);
            final Set<Path> permissionExpand = permissionExpand(schema, read);
            callerExpand.addAll(Path.children(permissionExpand, Path.of(VAR_CALLER)));
            final Set<Path> readExpand = Sets.union(Path.children(permissionExpand, Path.of(VAR_THIS)), Nullsafe.option(expand));
            transientExpand.addAll(schema.transientExpand(Path.of(), readExpand));
        }

        return expandCaller(Context.init(), caller, callerExpand)
                .thenCompose(expandedCaller -> expand(context(expandedCaller), instances, transientExpand)
                .thenApply(vs -> vs.map(v -> restrict(caller, v, expand))));
    }

    @Override
    public CompletableFuture<Instance> create(final Caller caller, final CreateOptions options) {

        log.debug("Create: options={}", options);

        return single(caller, new CreateAction(objectSchema(options.getSchema()), options));
    }

    @Override
    public CompletableFuture<Instance> update(final Caller caller, final UpdateOptions options) {

        log.debug("Update: options={}", options);

        return single(caller, new UpdateAction(objectSchema(options.getSchema()), options));
    }

    @Override
    public CompletableFuture<Instance> delete(final Caller caller, final DeleteOptions options) {

        log.debug("Delete: options={}", options);

        return single(caller, new DeleteAction(objectSchema(options.getSchema()), options));
    }

    @Override
    public CompletableFuture<PagedList<Instance>> queryLink(final Caller caller, final QueryLinkOptions options) {

        log.debug("Query link: options={}", options);

        final ObjectSchema ownerSchema = namespace.requireObjectSchema(options.getSchema());
        final Link link = ownerSchema.requireLink(options.getLink(), true);
        final String ownerId = options.getId();

        return read(caller, ReadOptions.builder().schema(ownerSchema.getName()).id(ownerId).build())
                .thenCompose(owner -> {

                    if (owner == null) {
                        throw new ObjectMissingException(ownerSchema.getName(), ownerId);
                    }

                    final int count = MoreObjects.firstNonNull(options.getCount(), QueryLinkOptions.DEFAULT_COUNT);
                    if(count > QueryLinkOptions.MAX_COUNT) {
                        throw new IllegalStateException("Count too high");
                    }
                    final PagingToken paging = options.getPaging();

                    return queryLinkImpl(context(caller), link, owner, count, paging)
                            .thenCompose(results -> expandAndRestrict(caller, results, options.getExpand()));
                });
    }

    @Override
    public CompletableFuture<PagedList<Instance>> query(final Caller caller, final QueryOptions options) {

        log.debug("Query: options={}", options);

        final ObjectSchema objectSchema = objectSchema(options.getSchema());
        final Expression expression = options.getExpression();

        final Permission permission = objectSchema.getPermission(Permission.READ);

        final Context context = context(caller, ImmutableMap.of());

        final Expression rooted;
        if(expression != null) {
            rooted = expression.bind(Context.init(), PathTransform.root(Path.of(Reserved.THIS)));
        } else {
            rooted = new Constant(true);
        }

        final Expression merged;
        if(permission != null && !caller.isSuper()) {
            merged = new And(permission.getExpression(), rooted);
        } else {
            merged = rooted;
        }

        final Expression bound = merged.bind(context);

        final List<Sort> sort = MoreObjects.firstNonNull(options.getSort(), Collections.emptyList());
        final int count = MoreObjects.firstNonNull(options.getCount(), QueryOptions.DEFAULT_COUNT);
        if(count > QueryOptions.MAX_COUNT) {
            throw new IllegalStateException("Count too high");
        }
        final PagingToken paging = options.getPaging();

        final Expression unrooted = bound.bind(Context.init(), PathTransform.unroot(Path.of(Reserved.THIS)));

        return queryImpl(context, objectSchema, unrooted, sort, count, paging)
                .thenCompose(results -> expandAndRestrict(caller, results, options.getExpand()));
    }

    protected void checkPermission(final Caller caller, final ObjectSchema schema, final Permission permission, final Map<String, Object> scope) {

        if(caller.isAnon()) {
            if(permission == null || !permission.isAnonymous()) {
                throw new PermissionDeniedException("Anonymous not allowed");
            }
        }
        if (!caller.isSuper() && permission != null) {
            final Context context = context(caller, scope);
            try {
                log.debug("Checking permission {}", permission.getExpression());
                if (permission.getExpression().evaluatePredicate(context)) {
                    return;
                }
            } catch (final Exception e) {
                throw new PermissionDeniedException(permission.getExpression().toString(), e);
            }
            throw new PermissionDeniedException(permission.getExpression().toString());
        }
    }

    private Context context(final Caller caller) {

        return context(caller, ImmutableMap.of());
    }

    private Context context(final Caller caller, final Map<String, Object> scope) {

        final Map<String, Object> fullScope = new HashMap<>(scope);
        fullScope.put(VAR_CALLER, ExpandedCaller.getObject(caller));
        return Context.init(fullScope);
    }

    protected CompletableFuture<?> onObjectCreated(final ObjectCreatedEvent event) {

        final ObjectSchema schema = objectSchema(event.getSchema());
        final StorageTraits traits = storage.storageTraits(schema);
        final String id = event.getId();
        final Map<String, Object> after = event.getAfter();
        final Long afterVersion = Instance.getVersion(after);
        assert afterVersion != null;
        final Set<Event> events = new HashSet<>();
        events.addAll(historyEvents(schema, id, after));
        events.addAll(schema.getIndexes().values().stream().flatMap(index -> {
            final Consistency best = traits.getIndexConsistency(index.isMultiValue());
            if(index.getConsistency(best).isAsync()) {
                final Map<Index.Key, Map<String, Object>> records = index.readValues(after);
                return records.entrySet().stream()
                        .map(e -> AsyncIndexCreatedEvent.of(schema.getName(), index.getName(), id, 0L, e.getKey(), e.getValue()));
            } else {
                return Stream.empty();
            }
        }).collect(Collectors.toSet()));
        events.addAll(refQueryEvents(schema, id, after));
        return emitter.emit(events);
    }

    protected CompletableFuture<?> onObjectUpdated(final ObjectUpdatedEvent event) {

        final ObjectSchema schema = objectSchema(event.getSchema());
        final String id = event.getId();
        final Map<String, Object> before = event.getBefore();
        final Map<String, Object> after = event.getAfter();
        final Set<Event> events = new HashSet<>();
        events.addAll(refreshObjectEvents(schema, id, before, after));
        events.addAll(refQueryEvents(schema, id, after));
        return emitter.emit(events);
    }

    protected CompletableFuture<?> onObjectDeleted(final ObjectDeletedEvent event) {

        final ObjectSchema schema = objectSchema(event.getSchema());
        final StorageTraits traits = storage.storageTraits(schema);
        final String id = event.getId();
        final long version = event.getVersion();
        final Map<String, Object> before = event.getBefore();
        return emitter.emit(schema.getIndexes().values().stream().flatMap(index -> {
            final Consistency best = traits.getIndexConsistency(index.isMultiValue());
            if(index.getConsistency(best).isAsync()) {
                final Map<Index.Key, Map<String, Object>> records = index.readValues(before);
                return records.keySet().stream()
                        .map(key -> AsyncIndexDeletedEvent.of(schema.getName(), index.getName(), id, version, key));
            } else {
                return Stream.empty();
            }
        }).collect(Collectors.toSet()));
    }

    protected CompletableFuture<?> onObjectRefreshed(final ObjectRefreshedEvent event) {

        final ObjectSchema schema = objectSchema(event.getSchema());
        final String id = event.getId();
        final Map<String, Object> before = event.getBefore();
        final Map<String, Object> after = event.getAfter();
        final Set<Event> events = new HashSet<>(refreshObjectEvents(schema, id, before, after));
        return emitter.emit(events);
    }

    protected CompletableFuture<?> onAsyncIndexCreated(final AsyncIndexCreatedEvent event) {

        final ObjectSchema schema = objectSchema(event.getSchema());
        final Index index = schema.requireIndex(event.getIndex(), true);
        final Storage.WriteTransaction write = storage.write(Consistency.ASYNC);
        write.createIndex(schema, index, event.getId(), event.getVersion(), event.getKey(), event.getProjection());
        return write.commit();
    }

    protected CompletableFuture<?> onAsyncIndexUpdated(final AsyncIndexUpdatedEvent event) {

        final ObjectSchema schema = objectSchema(event.getSchema());
        final Index index = schema.requireIndex(event.getIndex(), true);
        final Storage.WriteTransaction write = storage.write(Consistency.ASYNC);
        write.updateIndex(schema, index, event.getId(), event.getVersion(), event.getKey(), event.getProjection());
        return write.commit();
    }

    protected CompletableFuture<?> onAsyncIndexDeleted(final AsyncIndexDeletedEvent event) {

        final ObjectSchema schema = objectSchema(event.getSchema());
        final Index index = schema.requireIndex(event.getIndex(), true);
        final Storage.WriteTransaction write = storage.write(Consistency.ASYNC);
        write.deleteIndex(schema, index, event.getId(), event.getVersion(), event.getKey());
        return write.commit();
    }

    protected CompletableFuture<?> onAsyncHistoryCreated(final AsyncHistoryCreatedEvent event) {

        final ObjectSchema schema = objectSchema(event.getSchema());
        final Storage.WriteTransaction write = storage.write(Consistency.ASYNC);
        write.createHistory(schema, event.getId(), event.getVersion(), event.getAfter());
        return write.commit();
    }

    protected CompletableFuture<?> onRefQuery(final RefQueryEvent event) {

        final ObjectSchema schema = objectSchema(event.getSchema());
        final CompletableFuture<PagedList<Instance>> query = queryImpl(context(Caller.SUPER), schema,
                event.getExpression(), ImmutableList.of(), REF_QUERY_BATCH_SIZE, event.getPaging());
        return query.thenApply(page -> {
            final Set<Event> events = new HashSet<>();
            page.forEach(instance -> events.add(RefRefreshEvent.of(event.getRef(), schema.getName(), Instance.getId(instance))));
            if(page.hasPaging()) {
                events.add(event.withPaging(page.getPaging()));
            }
            return emitter.emit(events);
        });
    }

    protected CompletableFuture<?> onRefRefresh(final RefRefreshEvent event) {

        final ObjectSchema schema = objectSchema(event.getSchema());
        final String id = event.getId();
        final ObjectSchema refSchema = objectSchema(event.getRef().getSchema());
        final String refId = event.getRef().getId();
        final Storage.ReadTransaction read = storage.read(Consistency.ATOMIC);
        read.readObject(schema, id);
        read.readObject(refSchema, refId);
        return read.read().thenCompose(readResponse -> {
            final Instance before = schema.create(readResponse.getObject(schema, id), true, true);
            final Instance refAfter = refSchema.create(readResponse.getObject(refSchema, refId), true, true);
            if(before != null && refAfter != null) {
                final Long version = Instance.getVersion(before);
                assert version != null;
                final Instance after = schema.expand(before, new Expander() {
                    @Override
                    public Instance expandRef(final ObjectSchema schema, final Instance ref, final Set<Path> expand) {

                        if(schema.getName().equals(refSchema.getName())) {
                            if(refId.equals(Instance.getId(ref))) {
                                return refAfter;
                            }
                        }
                        return ref;
                    }

                    @Override
                    public PagedList<Instance> expandLink(final Link link, final PagedList<Instance> value, final Set<Path> expand) {

                        return value;
                    }
                }, schema.getExpand());
                final Storage.WriteTransaction write = storage.write(Consistency.ATOMIC);
                write.updateObject(schema, id, before, after);
                return write.commit()
                        .thenCompose(ignored -> emitter.emit(ObjectRefreshedEvent.of(schema.getName(), id, version, before, after)));
            } else {
                return CompletableFuture.completedFuture(null);
            }
        });
    }

    private Set<Event> refQueryEvents(final ObjectSchema schema, final String id, final Map<String, Object> after) {

        final Set<Event> events = new HashSet<>();
        namespace.forEachObjectSchema((k, v) -> {
            final Set<Expression> queries = v.refQueries(schema.getName());
            if(!queries.isEmpty()) {
                final Or merged = new Or(queries.toArray(new Expression[0]));
                final Expression bound = merged.bind(context(Caller.ANON, ImmutableMap.of(Reserved.THIS, after)));
                events.add(RefQueryEvent.of(Ref.of(schema.getName(), id), k, bound));
            }
        });
        return events;
    }

    private Set<Event> historyEvents(final ObjectSchema schema, final String id, final Map<String, Object> after) {

        final Long afterVersion = Instance.getVersion(after);
        assert afterVersion != null;
        final StorageTraits traits = storage.storageTraits(schema);
        final History history = schema.getHistory();
        if(history.isEnabled() && history.getConsistency(traits.getHistoryConsistency()).isAsync()) {
            return Collections.singleton(AsyncHistoryCreatedEvent.of(schema.getName(), id, afterVersion, after));
        } else {
            return Collections.emptySet();
        }
    }

    private Set<Event> refreshObjectEvents(final ObjectSchema schema, final String id, final Map<String, Object> before, final Map<String, Object> after) {

        final StorageTraits traits = storage.storageTraits(schema);
        final Long beforeVersion = Instance.getVersion(before);
        final Long afterVersion = Instance.getVersion(after);
        assert beforeVersion != null && afterVersion != null;
        final Set<Event> events = new HashSet<>();
        events.addAll(historyEvents(schema, id, after));
        events.addAll(schema.getIndexes().values().stream().flatMap(index -> {
            final Consistency best = traits.getIndexConsistency(index.isMultiValue());
            if(index.getConsistency(best).isAsync()) {
                final IndexRecordDiff diff = IndexRecordDiff.from(index.readValues(before), index.readValues(after));
                final Stream<Event> create = diff.getCreate().entrySet().stream()
                        .map(e -> AsyncIndexCreatedEvent.of(schema.getName(), index.getName(), id, beforeVersion, e.getKey(), e.getValue()));
                final Stream<Event> update = diff.getUpdate().entrySet().stream()
                        .map(e -> AsyncIndexUpdatedEvent.of(schema.getName(), index.getName(), id, beforeVersion, e.getKey(), e.getValue()));
                final Stream<Event> delete = diff.getDelete().stream()
                        .map(key-> AsyncIndexDeletedEvent.of(schema.getName(), index.getName(), id, beforeVersion, key));
                return Stream.of(create, update, delete)
                        .flatMap(v -> v);
            } else {
                return Stream.empty();
            }
        }).collect(Collectors.toSet()));
        return events;
    }
}

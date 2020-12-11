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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.basestar.auth.Caller;
import io.basestar.auth.exception.PermissionDeniedException;
import io.basestar.database.action.Action;
import io.basestar.database.action.CreateAction;
import io.basestar.database.action.DeleteAction;
import io.basestar.database.action.UpdateAction;
import io.basestar.database.event.*;
import io.basestar.database.exception.BatchKeyRepeatedException;
import io.basestar.database.exception.DatabaseReadonlyException;
import io.basestar.database.options.*;
import io.basestar.database.util.ExpandKey;
import io.basestar.database.util.RefKey;
import io.basestar.event.Emitter;
import io.basestar.event.Event;
import io.basestar.event.Handler;
import io.basestar.event.Handlers;
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.expression.Renaming;
import io.basestar.expression.constant.Constant;
import io.basestar.expression.logical.And;
import io.basestar.expression.logical.Or;
import io.basestar.schema.*;
import io.basestar.schema.secret.SecretContext;
import io.basestar.schema.use.ValueContext;
import io.basestar.schema.util.Expander;
import io.basestar.schema.util.Ref;
import io.basestar.storage.ConstantStorage;
import io.basestar.storage.Storage;
import io.basestar.storage.Versioning;
import io.basestar.storage.exception.ObjectMissingException;
import io.basestar.storage.overlay.OverlayStorage;
import io.basestar.util.*;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@Slf4j
public class DatabaseServer extends ReadProcessor implements Database, Handler<Event>, CommonVars {

    private static final String SINGLE_BATCH_ROOT = "$";

    private static final int REF_QUERY_BATCH_SIZE = 100;

    private final Emitter emitter;

    private final DatabaseMode mode;

    private final SecretContext secretContext;

    private static final Handlers<DatabaseServer> HANDLERS = Handlers.<DatabaseServer>builder()
            .on(ObjectCreatedEvent.class, DatabaseServer::onObjectCreated)
            .on(ObjectUpdatedEvent.class, DatabaseServer::onObjectUpdated)
            .on(ObjectDeletedEvent.class, DatabaseServer::onObjectDeleted)
            .on(ObjectRefreshedEvent.class, DatabaseServer::onObjectRefreshed)
            .on(RefQueryEvent.class, DatabaseServer::onRefQuery)
            .on(RefRefreshEvent.class, DatabaseServer::onRefRefresh)
            .build();

    @lombok.Builder(builderClassName = "Builder")
    protected DatabaseServer(final Namespace namespace, final Storage storage,
                             final Emitter emitter, final DatabaseMode mode,
                             final SecretContext secretContext) {

        super(namespace, storage);
        this.emitter = Nullsafe.orDefault(emitter, Emitter::skip);
        this.mode = Nullsafe.orDefault(mode, DatabaseMode.DEFAULT);
        this.secretContext = Nullsafe.orDefault(secretContext, SecretContext::none);
    }

    @Override
    public CompletableFuture<?> handle(final Event event, final Map<String, String> meta) {

        return HANDLERS.handle(this, event, meta);
    }

    @Override
    public Namespace namespace() {

        return namespace;
    }

    @Override
    public CompletableFuture<Map<String, Instance>> batch(final Caller caller, final BatchOptions options) {

        log.debug("Batch: options={}", options);

        final Map<String, Action> actions = new HashMap<>();
        options.getActions().forEach((name, action) -> {
            if (action instanceof CreateOptions) {
                final CreateOptions create = (CreateOptions)action;
                actions.put(name, new CreateAction(namespace.requireObjectSchema(create.getSchema()), create));
            } else if (action instanceof UpdateOptions) {
                final UpdateOptions update = (UpdateOptions)action;
                actions.put(name, new UpdateAction(namespace.requireObjectSchema(update.getSchema()), update));
            } else if (action instanceof DeleteOptions) {
                final DeleteOptions delete = (DeleteOptions)action;
                actions.put(name, new DeleteAction(namespace.requireObjectSchema(delete.getSchema()), delete));
            }
        });

        return batch(caller, options.getConsistency(), actions);
    }

    private CompletableFuture<Instance> single(final Caller caller, final Action action) {

        // FIXME: unspecified consistency should be passed to storage so it can define default, but that might break too many things today
        return batch(caller, Nullsafe.orDefault(action.getConsistency(), Consistency.ATOMIC), ImmutableMap.of(SINGLE_BATCH_ROOT, action))
                .thenApply(v -> v.get(SINGLE_BATCH_ROOT));
    }

    private Set<Name> permissionExpand(final ReferableSchema schema, final Permission permission) {

        return permission == null ? Collections.emptySet() : Nullsafe.orDefault(permission.getExpand());
    }

    private CompletableFuture<Map<String, Instance>> batch(final Caller caller, final Consistency consistency, final Map<String, Action> actions) {

        final ValueContext valueContext = secretContext.encryptingValueContext();

        final Set<RefKey> beforeCheck = new HashSet<>();
        final Set<ExpandKey<RefKey>> beforeKeys = new HashSet<>();
        final Set<Name> beforeCallerExpand = new HashSet<>();

        if(mode.isReadonly()) {
            throw new DatabaseReadonlyException();
        }

        actions.forEach((name, action) -> {
            action.validate();
            final String id = action.id();
            if (id != null) {
                final ObjectSchema schema = action.schema();
                final RefKey key = new RefKey(schema.getQualifiedName(), id, null);
                if (!beforeCheck.add(key)) {
                    throw new BatchKeyRepeatedException(key.getSchema(), key.getId());
                }
                final Set<Name> permissionExpand = permissionExpand(schema, schema.getPermission(Permission.READ));
                beforeCallerExpand.addAll(Name.children(permissionExpand, Name.of(VAR_CALLER)));
                final Set<Name> readExpand = Name.children(permissionExpand, Name.of(VAR_BEFORE));
                final Set<Name> transientExpand = schema.transientExpand(Name.of(), readExpand);
                beforeKeys.add(ExpandKey.from(key, transientExpand));
            }
        });

        return expandCaller(Context.init(), caller, beforeCallerExpand).thenCompose(beforeCaller -> {

            final Context beforeContext = context(beforeCaller);

            // Read initial values

            final CompletableFuture<Map<RefKey, Instance>> beforeFuture;
            if (!beforeKeys.isEmpty()) {
                final Storage.ReadTransaction read = storage.read(consistency);
                beforeKeys.forEach(expandKey -> {
                    final RefKey key = expandKey.getKey();
                    final ReferableSchema objectSchema = referableSchema(key.getSchema());
                    read.get(objectSchema, key.getId(), expandKey.getExpand());
                });
                beforeFuture = read.read().thenCompose(readResults -> {

                    final Map<ExpandKey<RefKey>, Instance> beforeUnexpanded = new HashMap<>();
                    beforeKeys.forEach(expandKey -> {
                        final RefKey key = expandKey.getKey();
                        final Map<String, Object> data = readResults.get(key.getSchema(), key.getId());
                        if (data != null) {
                            beforeUnexpanded.put(expandKey, referableSchema(key.getSchema()).create(data));
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
                final Set<Name> afterCallerExpand = new HashSet<>();

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
                        beforeKey = new RefKey(schema.getQualifiedName(), id, null);
                        before = beforeResults.get(beforeKey);
                    } else {
                        beforeKey = null;
                        before = null;
                    }
                    final Instance after = action.after(valueContext, beforeContext.with(VAR_BATCH, overlay), before);
                    if (after != null) {
                        final RefKey afterKey = RefKey.latest(after);
                        if (!afterCheck.add(afterKey)) {
                            throw new BatchKeyRepeatedException(afterKey.getSchema(), afterKey.getId());
                        }
                        resultLookup.put(name, afterKey);
                        overlay.put(name, after);
                        final Set<Name> permissionExpand = permissionExpand(schema, action.permission(before));
                        afterCallerExpand.addAll(Name.children(permissionExpand, Name.of(VAR_CALLER)));
                        final Set<Name> readExpand = Sets.union(
                                Name.children(permissionExpand, Name.of(VAR_AFTER)),
                                Nullsafe.orDefault(action.afterExpand())
                        );
                        final Set<Name> transientExpand = schema.transientExpand(Name.of(), readExpand);
                        final Set<Name> writeExpand = Sets.union(transientExpand, schema.getExpand());
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

                final Storage overlayStorage = OverlayStorage.builder()
                        .baseline(storage)
                        .overlay(ConstantStorage.builder().items(overlay.values()).build())
                        .build();

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
                            final Instance before = beforeResults.get(key);
                            final Instance after = afterResults.get(key);
                            final Permission permission = action.permission(before);
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

                        final Storage.WriteTransaction write = storage.write(consistency, Versioning.CHECKED);

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
                                final Instance expanded = schema.expand(after, Expander.noop(), Nullsafe.orDefault(action.afterExpand()));
                                restricted = schema.applyVisibility(afterContext, expanded);
                            }
                            results.put(name, restricted);
                        });

                        return write.write()
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
            final Set<Name> names = Name.children(action.paths(), Name.of(VAR_BATCH));
            final Set<String> matches = names.stream().map(AbstractPath::first)
                    .filter(actions::containsKey)
                    .collect(Collectors.toSet());
            dependencies.put(name, matches);
        });

        return TopologicalSort.stableSort(dependencies.keySet(), dependencies::get);
    }

    private void writeCreate(final Storage.WriteTransaction write, final ObjectSchema schema, final String id, final Instance after) {

        schema.validateObject(id, after);
        write.createObject(schema, id, after);
    }

    private void writeUpdate(final Storage.WriteTransaction write, final ObjectSchema schema, final String id, final Instance before, final Instance after) {

        schema.validateObject(id, after);
        write.updateObject(schema, id, before, after);
    }

    private void writeDelete(final Storage.WriteTransaction write, final ObjectSchema schema, final String id, final Instance before) {

        write.deleteObject(schema, id, before);
    }

    @Override
    public CompletableFuture<Instance> read(final Caller caller, final ReadOptions options) {

        log.debug("Read: options={}", options);

        final String id = options.getId();
        final ReferableSchema objectSchema = namespace.requireReferableSchema(options.getSchema());

        return readImpl(objectSchema, id, options.getVersion(), options.getExpand())
                .thenCompose(initial -> expandAndRestrict(caller, initial, options.getExpand()));
    }

    // FIXME need to apply nested permissions
    private Instance restrict(final Caller caller, final Instance instance, final Set<Name> expand) {

        final ReferableSchema schema = referableSchema(Instance.getSchema(instance));
        final Permission read = schema.getPermission(Permission.READ);
        checkPermission(caller, schema, read, ImmutableMap.of(VAR_THIS, instance));
        final Instance visible = schema.applyVisibility(context(caller), instance);
        return schema.expand(visible, Expander.noop(), expand);
    }

    // FIXME need to create a deeper permission expand for nested permissions
    private CompletableFuture<Instance> expandAndRestrict(final Caller caller, final Instance instance, final Set<Name> expand) {

        if(instance == null) {
            return CompletableFuture.completedFuture(null);
        }

        final ReferableSchema schema = referableSchema(Instance.getSchema(instance));
        final Permission read = schema.getPermission(Permission.READ);
        final Set<Name> permissionExpand = permissionExpand(schema, read);
        final Set<Name> callerExpand = Name.children(permissionExpand, Name.of(VAR_CALLER));
        final Set<Name> readExpand = Sets.union(Name.children(permissionExpand, Name.of(VAR_THIS)), Nullsafe.orDefault(expand));
        final Set<Name> transientExpand = schema.transientExpand(Name.of(), readExpand);

        return expandCaller(Context.init(), caller, callerExpand)
                .thenCompose(expandedCaller -> expand(context(expandedCaller), instance, transientExpand)
                .thenApply(v -> restrict(expandedCaller, v, expand)));
    }

    // FIXME need to create a deeper permission expand for nested permissions
    private CompletableFuture<Page<Instance>> expandAndRestrict(final Caller caller, final Page<Instance> instances, final Set<Name> expand) {

        final Set<Name> callerExpand = new HashSet<>();
        final Set<Name> transientExpand = new HashSet<>();
        for(final Instance instance : instances) {
            final ReferableSchema schema = referableSchema(Instance.getSchema(instance));
            final Permission read = schema.getPermission(Permission.READ);
            final Set<Name> permissionExpand = permissionExpand(schema, read);
            callerExpand.addAll(Name.children(permissionExpand, Name.of(VAR_CALLER)));
            final Set<Name> readExpand = Sets.union(Name.children(permissionExpand, Name.of(VAR_THIS)), Nullsafe.orDefault(expand));
            transientExpand.addAll(schema.transientExpand(Name.of(), readExpand));
        }

        return expandCaller(Context.init(), caller, callerExpand)
                .thenCompose(expandedCaller -> expand(context(expandedCaller), instances, transientExpand)
                .thenApply(vs -> vs.map(v -> restrict(expandedCaller, v, expand))));
    }

    @Override
    public CompletableFuture<Instance> create(final Caller caller, final CreateOptions options) {

        log.debug("Create: options={}", options);

        return single(caller, new CreateAction(namespace.requireObjectSchema(options.getSchema()), options));
    }

    @Override
    public CompletableFuture<Instance> update(final Caller caller, final UpdateOptions options) {

        log.debug("Update: options={}", options);

        return single(caller, new UpdateAction(namespace.requireObjectSchema(options.getSchema()), options));
    }

    @Override
    public CompletableFuture<Instance> delete(final Caller caller, final DeleteOptions options) {

        log.debug("Delete: options={}", options);

        return single(caller, new DeleteAction(namespace.requireObjectSchema(options.getSchema()), options));
    }

    @Override
    public CompletableFuture<Page<Instance>> queryLink(final Caller caller, final QueryLinkOptions options) {

        log.debug("Query link: options={}", options);

        final ObjectSchema ownerSchema = namespace.requireObjectSchema(options.getSchema());
        final Link link = ownerSchema.requireLink(options.getLink(), true);
        final String ownerId = options.getId();

        return read(caller, ReadOptions.builder().schema(ownerSchema.getQualifiedName()).id(ownerId).build())
                .thenCompose(owner -> {

                    if (owner == null) {
                        throw new ObjectMissingException(ownerSchema.getQualifiedName(), ownerId);
                    }

                    final int count = Nullsafe.orDefault(options.getCount(), QueryLinkOptions.DEFAULT_COUNT);
                    if(count > QueryLinkOptions.MAX_COUNT) {
                        throw new IllegalStateException("Count too high (max " +  QueryLinkOptions.MAX_COUNT + ")");
                    }
                    final Page.Token paging = options.getPaging();

                    return queryLinkImpl(context(caller), link, owner, options.getExpand(), count, paging)
                            .thenCompose(results -> expandAndRestrict(caller, results, options.getExpand()));
                });
    }

    @Override
    public CompletableFuture<Page<Instance>> query(final Caller caller, final QueryOptions options) {

        log.debug("Query: options={}", options);

        final InstanceSchema schema = namespace.requireInstanceSchema(options.getSchema());

        final int count = Nullsafe.orDefault(options.getCount(), QueryOptions.DEFAULT_COUNT);
        if (count > QueryOptions.MAX_COUNT) {
            throw new IllegalStateException("Count too high (max " + QueryLinkOptions.MAX_COUNT + ")");
        }
        final Page.Token paging = options.getPaging();

        if(schema instanceof ViewSchema) {

            throw new UnsupportedOperationException();

        } else if(schema instanceof ReferableSchema) {

            final ReferableSchema objectSchema = (ReferableSchema)schema;
            final Expression expression = options.getExpression();

            final Permission permission = objectSchema.getPermission(Permission.READ);

            final Context context = context(caller, ImmutableMap.of());

            final Expression rooted;
            if (expression != null) {
                rooted = expression.bind(Context.init(), Renaming.addPrefix(Name.of(Reserved.THIS)));
            } else {
                rooted = new Constant(true);
            }

            final Expression merged;
            if (permission != null && !caller.isSuper()) {
                merged = new And(permission.getExpression(), rooted);
            } else {
                merged = rooted;
            }

            final Expression bound = merged.bind(context);

            final List<Sort> sort = Nullsafe.orDefault(options.getSort(), Collections.emptyList());
            final Expression unrooted = bound.bind(Context.init(), Renaming.removeExpectedPrefix(Name.of(Reserved.THIS)));

            return queryImpl(context, objectSchema, unrooted, sort, options.getExpand(), count, paging)
                    .thenCompose(results -> expandAndRestrict(caller, results, options.getExpand()));

        } else {
            throw new IllegalStateException(options.getSchema() + " is not an object or view schema");
        }
    }

    protected void checkPermission(final Caller caller, final ReferableSchema schema, final Permission permission, final Map<String, Object> scope) {

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

        final ObjectSchema schema = namespace.requireObjectSchema(event.getSchema());
        final String id = event.getId();
        final Map<String, Object> after = event.getAfter();
        return storage.afterCreate(schema, id, after)
                .thenCompose(events -> emitter.emit(Immutable.copyAddAll(events, refQueryEvents(schema, id))));
    }

    protected CompletableFuture<?> onObjectUpdated(final ObjectUpdatedEvent event) {

        final ObjectSchema schema = namespace.requireObjectSchema(event.getSchema());
        final String id = event.getId();
        final long version = event.getVersion();
        final Map<String, Object> before = event.getBefore();
        final Map<String, Object> after = event.getAfter();
        return storage.afterUpdate(schema, id, version, before, after)
                .thenCompose(events -> emitter.emit(Immutable.copyAddAll(events, refQueryEvents(schema, id))));
    }

    protected CompletableFuture<?> onObjectDeleted(final ObjectDeletedEvent event) {

        final ObjectSchema schema = namespace.requireObjectSchema(event.getSchema());
        final String id = event.getId();
        final long version = event.getVersion();
        final Map<String, Object> before = event.getBefore();
        return storage.afterDelete(schema, id, version, before)
                .thenCompose(events -> emitter.emit(Immutable.copyAddAll(events, refQueryEvents(schema, id))));
    }

    protected CompletableFuture<?> onObjectRefreshed(final ObjectRefreshedEvent event) {

        final ObjectSchema schema = namespace.requireObjectSchema(event.getSchema());
        final String id = event.getId();
        final Map<String, Object> before = event.getBefore();
        final Map<String, Object> after = event.getAfter();
        final long version = event.getVersion();
        return storage.afterUpdate(schema, id, version, before, after)
                .thenCompose(emitter::emit);
    }

    protected CompletableFuture<?> onRefQuery(final RefQueryEvent event) {

        final ObjectSchema schema = namespace.requireObjectSchema(event.getSchema());
        final CompletableFuture<Page<Instance>> query = queryImpl(context(Caller.SUPER), schema,
                event.getExpression(), ImmutableList.of(), ImmutableSet.of(), REF_QUERY_BATCH_SIZE, event.getPaging());
        return query.thenApply(page -> {
            final Set<Event> events = new HashSet<>();
            page.forEach(instance -> events.add(RefRefreshEvent.of(event.getRef(), schema.getQualifiedName(), Instance.getId(instance))));
            if(page.hasMore()) {
                events.add(event.withPaging(page.getPaging()));
            }
            return emitter.emit(events);
        });
    }

    protected CompletableFuture<?> onRefRefresh(final RefRefreshEvent event) {

        final ObjectSchema schema = namespace.requireObjectSchema(event.getSchema());
        final String id = event.getId();
        final ObjectSchema refSchema = namespace.requireObjectSchema(event.getRef().getSchema());
        final String refId = event.getRef().getId();
        final Storage.ReadTransaction read = storage.read(Consistency.ATOMIC);
        read.get(schema, id, ImmutableSet.of());
        read.get(refSchema, refId, ImmutableSet.of());
        final Set<Name> expand = schema.getExpand();
        return read.read().thenCompose(readResponse -> {
            final Instance before = schema.create(readResponse.get(schema, id), expand, true);
            if(before != null) {
                final Set<Name> refExpand = schema.refExpand(refSchema.getQualifiedName(), schema.getExpand());
                final Instance refAfter = refSchema.create(readResponse.get(refSchema, refId), expand, true);
                final Long refAfterVersion = refAfter == null ? null : Instance.getVersion(refAfter);
                return expand(context(Caller.SUPER), refAfter, refExpand).thenCompose(expandedRefAfter -> {

                    final Long version = Instance.getVersion(before);
                    assert version != null;
                    final Instance after = schema.expand(before, new Expander() {
                        @Override
                        public Instance expandRef(final ReferableSchema schema, final Instance ref, final Set<Name> expand) {

                            if (ref == null) {
                                return null;
                            }
                            if (schema.getQualifiedName().equals(refSchema.getQualifiedName()) && refId.equals(Instance.getId(ref))) {
                                if (refAfter == null) {
                                    return ReferableSchema.ref(refId);
                                } else {
                                    return schema.expand(refAfter, Expander.noop(), expand);
                                }
                            }
                            return schema.expand(ref, this, expand);
                        }

                        @Override
                        public Instance expandVersionedRef(final ReferableSchema schema, final Instance ref, final Set<Name> expand) {

                            if (ref == null) {
                                return null;
                            }
                            final Long version = Instance.getVersion(ref);
                            if (schema.getQualifiedName().equals(refSchema.getQualifiedName())) {
                                if (refId.equals(Instance.getId(ref)) && version.equals(refAfterVersion)) {
                                    return schema.expand(refAfter, Expander.noop(), expand);
                                }
                            }
                            return schema.expand(ref, this, expand);
                        }

                        @Override
                        public Page<Instance> expandLink(final Link link, final Page<Instance> value, final Set<Name> expand) {

                            return value;
                        }
                    }, schema.getExpand());
                    final Storage.WriteTransaction write = storage.write(Consistency.ATOMIC, Versioning.CHECKED);
                    writeUpdate(write, schema, id, before, after);
                    return write.write()
                            .thenCompose(ignored -> emitter.emit(ObjectRefreshedEvent.of(schema.getQualifiedName(), id, version, before, after)));

                });
            } else {
                return CompletableFuture.completedFuture(null);
            }
        });
    }

    private Set<Event> refQueryEvents(final ObjectSchema schema, final String id) {

        final Set<Event> events = new HashSet<>();
        namespace.forEachObjectSchema((k, v) -> {
            final Set<Expression> queries = v.refQueries(schema.getQualifiedName(), v.getExpand());
            if(!queries.isEmpty()) {
                final Or merged = new Or(queries.toArray(new Expression[0]));
                final Expression bound = merged.bind(context(Caller.ANON, ImmutableMap.of(Reserved.THIS, ReferableSchema.ref(id))));
                events.add(RefQueryEvent.of(Ref.of(schema.getQualifiedName(), id), k, bound));
            }
        });
        return events;
    }
}

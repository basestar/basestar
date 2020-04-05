package io.basestar.database;

import com.google.common.base.MoreObjects;
import com.google.common.collect.*;
import io.basestar.auth.Caller;
import io.basestar.auth.exception.PermissionDeniedException;
import io.basestar.database.event.*;
import io.basestar.database.options.*;
import io.basestar.event.Emitter;
import io.basestar.event.Event;
import io.basestar.event.Handler;
import io.basestar.event.Handlers;
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.expression.PathTransform;
import io.basestar.expression.constant.Constant;
import io.basestar.expression.logical.And;
import io.basestar.expression.methods.Methods;
import io.basestar.schema.*;
import io.basestar.storage.Storage;
import io.basestar.storage.StorageTraits;
import io.basestar.storage.exception.ObjectMissingException;
import io.basestar.storage.util.IndexRecordDiff;
import io.basestar.storage.util.Pager;
import io.basestar.util.*;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class DatabaseServer implements Database, Handler<Event> {

    public static final String VAR_CALLER = "caller";

    public static final String VAR_BEFORE = "before";

    public static final String VAR_AFTER = "after";

    public static final String VAR_THIS = Reserved.THIS;

    public static final String CALLER_SCHEMA = "User";

    private static final boolean TOMBSTONE = false;

    private final Namespace namespace;

    private final Storage storage;

    private final Emitter emitter;

    private static final Handlers<DatabaseServer> HANDLERS = Handlers.<DatabaseServer>builder()
            .on(ObjectCreatedEvent.class, DatabaseServer::onObjectCreated)
            .on(ObjectUpdatedEvent.class, DatabaseServer::onObjectUpdated)
            .on(ObjectDeletedEvent.class, DatabaseServer::onObjectDeleted)
            .on(AsyncIndexCreatedEvent.class, DatabaseServer::onAsyncIndexCreated)
            .on(AsyncIndexUpdatedEvent.class, DatabaseServer::onAsyncIndexUpdated)
            .on(AsyncIndexDeletedEvent.class, DatabaseServer::onAsyncIndexDeleted)
            .on(AsyncIndexDeletedEvent.class, DatabaseServer::onAsyncIndexDeleted)
            .on(AsyncHistoryCreatedEvent.class, DatabaseServer::onAsyncHistoryCreated)
            .build();

    public DatabaseServer(final Namespace namespace, final Storage storage) {

        this(namespace, storage, Emitter.skip());
    }

    public DatabaseServer(final Namespace namespace, final Storage storage, final Emitter emitter) {

        this.namespace = namespace;
        this.storage = storage;
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
    public CompletableFuture<Instance> read(final Caller caller, final String schema, final String id, final ReadOptions options) {

        log.debug("Read: id={}, options={}", id, options);

        final ObjectSchema objectSchema = namespace.requireObjectSchema(schema);
        final Permission permission = objectSchema.getPermission(Permission.READ);

        return expandCaller(caller, permission)
                .thenCompose(expandedCaller -> {

                    // Will make 2 reads if the request schema doesn't match result schema

                    return readImpl(objectSchema, id, options.getVersion()).thenCompose(raw -> {

                        if (raw == null) {
                            return CompletableFuture.completedFuture(null);
                        } else {

                            return cast(objectSchema, raw).thenCompose(initial -> {

                                final ObjectSchema castSchema = schema(Instance.getSchema(initial));

                                final Set<Path> permissionExpand = readExpand(castSchema, permission, options.getExpand(), Path.of(VAR_THIS));
                                final Set<Path> readExpand = Nullsafe.of(options.getExpand());
                                final Set<Path> mergedExpand = Sets.union(readExpand, permissionExpand);
                                return expand(caller, initial, mergedExpand).thenApply(expanded -> {

                                    checkPermission(expandedCaller, castSchema, permission, ImmutableMap.of(
                                            VAR_THIS, expanded
                                    ));

                                    return castSchema.expand(expanded, Expander.noop(), readExpand);
                                });

                            });
                        }
                    });
                });
    }

    @Override
    public CompletableFuture<Instance> create(final Caller caller, final String schema, final String id, final Map<String, Object> data, final CreateOptions options) {

        log.debug("Create: id={}, data={}, options={}", id, data, options);

        final ObjectSchema objectSchema = namespace.requireObjectSchema(schema);
        final Storage.EventStrategy eventStrategy = storage.eventStrategy(objectSchema);
        final Permission permission = objectSchema.getPermission(Permission.CREATE);

        final Consistency consistency = Consistency.ATOMIC;

        return expandCaller(caller, permission)
                .thenCompose(expandedCaller -> {

                    final Map<String, Object> initial = new HashMap<>(objectSchema.create(data));

                    final LocalDateTime now = LocalDateTime.now();

                    Instance.setId(initial, id);
                    Instance.setVersion(initial, 1L);
                    Instance.setCreated(initial, now);
                    Instance.setUpdated(initial, now);
                    Instance.setHash(initial, objectSchema.hash(initial));

                    final Instance evaluated = objectSchema.evaluate(initial, context(expandedCaller, ImmutableMap.of(
                            VAR_THIS, initial
                    )));

                    objectSchema.validate(evaluated, context(expandedCaller, ImmutableMap.of(
                            VAR_THIS, evaluated
                    )));

                    final Set<Path> requestExpand = readExpand(objectSchema, permission, options.getExpand(), Path.of(VAR_AFTER));
                    return expand(expandedCaller, evaluated, requestExpand).thenCompose(after -> {

                        checkPermission(expandedCaller, objectSchema, permission, ImmutableMap.of(
                                VAR_AFTER, after
                        ));

                        final Storage.WriteTransaction writeTransaction = storage.write(consistency);

                        writeTransaction.createObject(objectSchema, id, after);

                        objectHierarchy(objectSchema).forEach(superSchema -> {

                            final Instance superAfter = superSchema.create(after);
                            writeTransaction.createObject(superSchema, id, superAfter);
                        });

                        return writeTransaction.commit()
                                .thenCompose(ignored -> emitCreateObject(eventStrategy, objectSchema, id, after))
                                .thenApply(ignored -> after);

                    });

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
    public CompletableFuture<Instance> update(final Caller caller, final String schema, final String id, final Map<String, Object> data, final UpdateOptions options) {

        return update(caller, schema, id, current -> {
            final UpdateOptions.Mode mode = MoreObjects.firstNonNull(options.getMode(), UpdateOptions.Mode.REPLACE);
            switch (mode) {
                case REPLACE:
                    return data;
                case MERGE:
                    final Map<String, Object> merged = new HashMap<>(current);
                    merged.putAll(data);
                    return merged;
                default:
                    throw new IllegalStateException();
            }
        }, options);
    }

    public CompletableFuture<Instance> update(final Caller caller, final String schema, final String id, final Function<Map<String, Object>, Map<String, Object>> data, final UpdateOptions options) {

        log.debug("Update: id={}, data={}, options={}", id, data, options);

        final ObjectSchema objectSchema = namespace.requireObjectSchema(schema);
        final Storage.EventStrategy eventStrategy = storage.eventStrategy(objectSchema);
        final Permission permission = objectSchema.getPermission(Permission.UPDATE);

        final Consistency consistency = Consistency.ATOMIC;

        return expandCaller(caller, permission)
                .thenCompose(expandedCaller -> {

                    final Set<Path> permissionExpand = readExpand(objectSchema, permission, Collections.emptySet(), Path.of(VAR_BEFORE));
                    return read(expandedCaller, schema, id, new ReadOptions().setExpand(permissionExpand)).thenCompose(before -> {

                        if (before == null) {
                            throw new IllegalStateException();
                        }

                        if(!Instance.getSchema(before).equals(objectSchema.getName())) {
                            throw new IllegalStateException("Cannot change instance schema");
                        }

                        final Map<String, Object> initial = new HashMap<>(objectSchema.create(data.apply(before)));

                        final LocalDateTime now = LocalDateTime.now();

                        final Long beforeVersion = Instance.getVersion(before);
                        assert beforeVersion != null;

                        final Long afterVersion = beforeVersion + 1;

                        Instance.setId(initial, id);
                        Instance.setVersion(initial, afterVersion);
                        Instance.setCreated(initial, Instance.getCreated(before));
                        Instance.setUpdated(initial, now);
                        Instance.setHash(initial, objectSchema.hash(initial));

                        final Instance evaluated = objectSchema.evaluate(initial, context(expandedCaller, ImmutableMap.of(
                                VAR_THIS, initial
                        )));

                        objectSchema.validate(before, evaluated, context(expandedCaller, ImmutableMap.of(
                                VAR_THIS, evaluated
                        )));

                        final Set<Path> requestExpand = readExpand(objectSchema, permission, options.getExpand(), Path.of(VAR_AFTER));
                        return expand(expandedCaller, evaluated, requestExpand).thenCompose(after -> {

                            checkPermission(expandedCaller, objectSchema, permission, ImmutableMap.of(
                                    VAR_BEFORE, before,
                                    VAR_AFTER, after
                            ));

                            final Storage.WriteTransaction writeTransaction = storage.write(consistency);

                            writeTransaction.updateObject(objectSchema, id, before, after);

                            objectHierarchy(objectSchema).forEach(superSchema -> {

                                final Instance superBefore = superSchema.create(before);
                                final Instance superAfter = superSchema.create(after);
                                writeTransaction.updateObject(superSchema, id, superBefore, superAfter);
                            });

                            return writeTransaction.commit()
                                    .thenCompose(ignored -> emitUpdateObject(eventStrategy, objectSchema, id, beforeVersion, before, after))
                                    .thenApply(ignored -> after);
                        });
                    });

                });
    }


    @Override
    public CompletableFuture<Boolean> delete(final Caller caller, final String schema, final String id, final DeleteOptions options) {

        log.debug("Delete: id={}, options={}", id, options);

        final ObjectSchema objectSchema = namespace.requireObjectSchema(schema);
        final Storage.EventStrategy eventStrategy = storage.eventStrategy(objectSchema);
        final Permission permission = objectSchema.getPermission(Permission.DELETE);

        final Consistency consistency = Consistency.ATOMIC;

        return expandCaller(caller, permission)
                .thenCompose(expandedCaller -> {

                    final Set<Path> permissionExpand = readExpand(objectSchema, permission, Collections.emptySet(), Path.of());
                    return read(expandedCaller, schema, id, new ReadOptions().setExpand(permissionExpand)).thenCompose(before -> {

                        if (before == null) {
                            throw new IllegalStateException();
                        }

                        if(!Instance.getSchema(before).equals(objectSchema.getName())) {
                            throw new IllegalStateException("Must delete using actual schema");
                        }

                        final Long beforeVersion = Instance.getVersion(before);
                        assert beforeVersion != null;

                        checkPermission(expandedCaller, objectSchema, permission, ImmutableMap.of(
                                VAR_BEFORE, before
                        ));

                        final Storage.WriteTransaction writeTransaction = storage.write(consistency);

                        if(TOMBSTONE) {

                            final long afterVersion = beforeVersion + 1;

                            final Map<String, Object> tombstone = new HashMap<>();
                            final LocalDateTime now = LocalDateTime.now();
                            Instance.setId(tombstone, id);
                            Instance.setVersion(tombstone, afterVersion);
                            Instance.setCreated(tombstone, Instance.getCreated(before));
                            Instance.setUpdated(tombstone, now);
                            Instance.setHash(tombstone, objectSchema.hash(tombstone));

                            objectHierarchy(objectSchema).forEach(superSchema -> {
                                final Instance superBefore = superSchema.create(before);
                                writeTransaction.updateObject(superSchema, id, superBefore, tombstone);
                            });
                        } else {
                            writeTransaction.deleteObject(objectSchema, id, before);

                            objectHierarchy(objectSchema).forEach(superSchema -> {
                                final Instance superBefore = superSchema.create(before);
                                writeTransaction.deleteObject(superSchema, id, superBefore);
                            });
                        }

                        return writeTransaction.commit()
                                .thenCompose(ignored -> emitDeleteObject(eventStrategy, objectSchema, id, beforeVersion, before))
                                .thenApply(ignored -> true);
                    });

                });
    }

    public CompletableFuture<?> emitCreateObject(final Storage.EventStrategy strategy, final ObjectSchema schema, final String id, final Map<String, Object> after) {

        if(strategy == Storage.EventStrategy.EMIT) {
            return emitter.emit(ObjectCreatedEvent.of(schema.getName(), id, after));
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }

    public CompletableFuture<?> emitUpdateObject(final Storage.EventStrategy strategy, final ObjectSchema schema, final String id, final long version, final Map<String, Object> before, final Map<String, Object> after) {

        if(strategy == Storage.EventStrategy.EMIT) {
            return emitter.emit(ObjectUpdatedEvent.of(schema.getName(), id, version, before, after));
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }

    public CompletableFuture<?> emitDeleteObject(final Storage.EventStrategy strategy, final ObjectSchema schema, final String id, final long version, final Map<String, Object> before) {

        if(strategy == Storage.EventStrategy.EMIT) {
            return emitter.emit(ObjectDeletedEvent.of(schema.getName(), id, version, before));
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }

    @Override
    public CompletableFuture<PagedList<Instance>> query(final Caller caller, final String schema, final Expression expression, final QueryOptions options) {

        log.debug("Query: schema={}, expression={}, options={}", schema, expression, options);

        final ObjectSchema objectSchema = namespace.requireObjectSchema(schema);
        return query(caller, objectSchema, expression, options);
    }

    protected CompletableFuture<PagedList<Instance>> query(final Caller caller, final ObjectSchema schema, final Expression expression, final QueryOptions options) {

        final Permission permission = schema.getPermission(Permission.READ);
        final Set<Path> expand = readExpand(schema, permission, options.getExpand(), Path.of(VAR_THIS));

        return expandCaller(caller, permission)
                .thenCompose(expandedCaller -> {

                    final Context context = context(expandedCaller, ImmutableMap.of());

                    final Expression rooted;
                    if(expression != null) {
                        rooted = expression.bind(context(), PathTransform.root(Path.of(Reserved.THIS)));
                    } else {
                        rooted = new Constant(true);
                    }

                    final Expression merged;
                    if(permission != null && !expandedCaller.isSuper()) {
                        merged = new And(permission.getExpression(), rooted);
                    } else {
                        merged = rooted;
                    }

                    final Expression bound = merged.bind(context);

                    final List<Sort> sort = MoreObjects.firstNonNull(options.getSort(), Collections.emptyList());
                    final int count = MoreObjects.firstNonNull(options.getCount(), QueryOptions.DEFAULT_COUNT);
                    final PagingToken paging = options.getPaging();

                    final List<Sort> pageSort = ImmutableList.<Sort>builder()
                                .addAll(sort)
                                .add(Sort.asc(Path.of(Reserved.ID)))
                                .build();

                    final Expression unrooted = bound.bind(context(), PathTransform.unroot(Path.of(Reserved.THIS)));

                    final List<Pager.Source<Instance>> sources = storage.query(schema, unrooted, sort).stream()
                            .map(source -> (Pager.Source<Instance>) (c, t) -> source.page(c, t)
                                    .thenCompose(data -> cast(schema, data))
                                    .thenApply(data -> data
                                            .filter(instance -> bound.evaluatePredicate(context.with(Reserved.THIS, instance)))))
                            .collect(Collectors.toList());

                    if(sources.isEmpty()) {
                        throw new IllegalStateException("Query not supported");
                    } else {

                        @SuppressWarnings("unchecked")
                        final Comparator<Instance> comparator = Sort.comparator(pageSort, (t, path) -> (Comparable)path.apply(t));
                        final Pager<Instance> pager = new Pager<>(comparator, sources, paging);
                        return pager.page(count)
                                .thenCompose(results -> expand(caller, results, expand));
                    }
                });
    }

    @Override
    public CompletableFuture<PagedList<Instance>> page(final Caller caller, final String schema, final String id, final String rel, final QueryOptions options) {

        log.debug("Query link: id={}, options={}", id, options);

        final ObjectSchema objectSchema = namespace.requireObjectSchema(schema);
        final Member member = objectSchema.requireMember(rel, true);

        if(member instanceof Link) {

            final Link link = (Link) member;
            final ObjectSchema linkSchema = link.getSchema();
            final Permission permission = linkSchema.getPermission(Permission.READ);

            return pageImpl(caller, permission, schema, id, link.getQuery(), link.getSchema(), link.getSort(), options);

        } else {
            throw new IllegalStateException("Member must be a link");
        }
    }

    private CompletableFuture<PagedList<Instance>> pageImpl(final Caller caller, final Permission permission,
                                                            final String ownerSchema, final String ownerId,
                                                            final Expression rawQuery, final ObjectSchema schema,
                                                            final List<Sort> sort, final QueryOptions options) {

        return expandCaller(caller, permission)
                .thenCompose(expandedCaller -> {

                    final QueryOptions queryOptions = new QueryOptions()
                            .setCount(options.getCount())
                            .setExpand(options.getExpand())
                            .setPaging(options.getPaging())
                            .setSort(sort);

                    return read(caller, ownerSchema, ownerId, new ReadOptions())
                            .thenCompose(owner -> {

                                if (owner == null) {
                                    throw new ObjectMissingException(ownerSchema, ownerId);
                                }

                                final Expression query = rawQuery
                                        .bind(context(caller, ImmutableMap.of(
                                                VAR_THIS, owner
                                        )));

                                return query(caller, schema, query, queryOptions);
                            });
                });
    }

    @Override
    public CompletableFuture<PagingToken> reindex(final Caller caller, final Map<String, ? extends Collection<String>> schemaIndexes, final int count, final PagingToken paging) {

        throw new UnsupportedOperationException();
        //        // FIXME
//        return storage.scanObjects(count, paging).thenApply(result -> {
//
//            for(final Map<String, Object> data : result) {
//                final String schema = ObjectSchema.getSchema(data);
//                final Collection<String> indexes = schemaIndexes.get(schema);
//                if(indexes != null && !indexes.isEmpty()) {
//
//                }
//            }
//
//            return result.getPaging();
//        });
    }

    private Set<Path> readExpand(final ObjectSchema schema, final Permission permission, final Set<Path> expand, final Path path) {

        final Set<Path> result = new HashSet<>();
        if(expand != null) {
            result.addAll(expand);
        }
        if(permission != null) {
//            final Set<Key> keys = Key.children(permission.getExpression().keys(), key);
//            final Set<Key> required = schema.requiredExpand(keys);
            result.addAll(Path.children(permission.getExpand(), path));
        }
        return result;
    }

    private void checkPermission(final Caller caller, final ObjectSchema schema, final Permission permission, final Map<String, Object> scope) {

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

    private Context context() {

        return Context.init(Methods.builder().defaults().build());
    }

    private Context context(final Map<String, Object> scope) {

        return Context.init(Methods.builder().defaults().build(), scope);
    }

    private Context context(final Caller caller, final Map<String, Object> scope) {

        final Map<String, Object> fullScope = new HashMap<>(scope);
        fullScope.put(VAR_CALLER, ExpandedCaller.getObject(caller));
        return context(fullScope);
    }

    private CompletableFuture<Map<String, Object>> readImpl(final ObjectSchema objectSchema, final String id, final Long version) {

        if (version == null) {
            return storage.readObject(objectSchema, id);
        } else {
            return storage.readObject(objectSchema, id)
                    .thenCompose(current -> {
                        if (current == null) {
                            return CompletableFuture.completedFuture(null);
                        } else {
                            final Long currentVersion = Instance.getVersion(current);
                            assert currentVersion != null;
                            if (currentVersion.equals(version)) {
                                return CompletableFuture.completedFuture(current);
                            } else if (version > currentVersion) {
                                return CompletableFuture.completedFuture(null);
                            } else {
                                return storage.readObjectVersion(objectSchema, id, version);
                            }
                        }
                    });
        }
    }

    private CompletableFuture<Instance> cast(final ObjectSchema baseSchema, final Map<String, Object> data) {

        final String castSchemaName = Instance.getSchema(data);
        if(baseSchema.getName().equals(castSchemaName)) {
            return CompletableFuture.completedFuture(baseSchema.create(data));
        } else {
            final String id = Instance.getId(data);
            final Long version = Instance.getVersion(data);
            final ObjectSchema castSchema = schema(castSchemaName);
            return readImpl(castSchema, id, version)
                    .thenApply(castSchema::create);
        }
    }

    private CompletableFuture<PagedList<Instance>> cast(final ObjectSchema baseSchema, final PagedList<? extends Map<String, Object>> data) {

        final Multimap<String, Map<String, Object>> needed = ArrayListMultimap.create();
        data.forEach(v -> {
            final String actualSchema = Instance.getSchema(v);
            if(!baseSchema.getName().equals(actualSchema)) {
                needed.put(actualSchema, v);
            }
        });
        if(needed.isEmpty()) {
            return CompletableFuture.completedFuture(data.map(v -> {
                final ObjectSchema schema = schema(Instance.getSchema(v));
                return schema.create(v);
            }));
        } else {
            final Storage.ReadTransaction readTransaction = storage.read(Consistency.NONE);
            needed.asMap().forEach((schemaName, items) -> {
                final ObjectSchema schema = schema(schemaName);
                items.forEach(item -> {
                    final String id = Instance.getId(item);
                    final Long version = Instance.getVersion(item);
                    assert version != null;
                    readTransaction.readObjectVersion(schema, id, version);
                });
            });
            return readTransaction.read().thenApply(results -> {

                final Map<RefKey, Map<String, Object>> mapped = new HashMap<>();
                results.forEach((key, result) -> mapped.put(new RefKey(key.getSchema(), key.getId()), result));

                return data.map(v -> {
                    final RefKey key = RefKey.from(v);
                    final Map<String, Object> result = MoreObjects.firstNonNull(mapped.get(key), v);
                    final ObjectSchema schema = schema(Instance.getSchema(result));
                    return schema.create(result);
                });
            });
        }
    }

    private CompletableFuture<Map<ExpandKey<RefKey>, Instance>> cast(final Map<ExpandKey<RefKey>, ? extends Map<String, Object>> data) {

        final Multimap<String, Map<String, Object>> needed = ArrayListMultimap.create();
        data.forEach((k, v) -> {
            final String actualSchema = Instance.getSchema(v);
            if(!k.getKey().getSchema().equals(actualSchema)) {
                needed.put(actualSchema, v);
            }
        });
        if(needed.isEmpty()) {
            final Map<ExpandKey<RefKey>, Instance> results = new HashMap<>();
            data.forEach((k, v) -> {
                final ObjectSchema schema = schema(Instance.getSchema(v));
                final Instance instance = schema.create(v);
                results.put(k, instance);
            });
            return CompletableFuture.completedFuture(results);
        } else {
            final Storage.ReadTransaction readTransaction = storage.read(Consistency.NONE);
            needed.asMap().forEach((schemaName, items) -> {
                final ObjectSchema schema = schema(schemaName);
                items.forEach(item -> {
                    final String id = Instance.getId(item);
                    final Long version = Instance.getVersion(item);
                    assert version != null;
                    readTransaction.readObjectVersion(schema, id, version);
                });
            });
            return readTransaction.read().thenApply(results -> {

                final Map<RefKey, Map<String, Object>> mapped = new HashMap<>();
                results.forEach((key, result) -> mapped.put(new RefKey(key.getSchema(), key.getId()), result));

                final Map<ExpandKey<RefKey>, Instance> remapped = new HashMap<>();
                data.forEach((k, v) ->  {
                    final RefKey key = RefKey.from(v);
                    final Map<String, Object> result = MoreObjects.firstNonNull(mapped.get(key), v);
                    final ObjectSchema schema = schema(Instance.getSchema(result));
                    final Instance instance = schema.create(result);
                    remapped.put(k, instance);
                });
                return remapped;
            });
        }
    }

    private CompletableFuture<Caller> expandCaller(final Caller caller, final Permission permission) {

        if(caller.isAnon()) {
            if(permission == null || !permission.isAnonymous()) {
                throw new PermissionDeniedException("Anonymous not allowed");
            }
        }

        if(caller.getId() == null || caller.isSuper()) {

            return CompletableFuture.completedFuture(caller instanceof ExpandedCaller ? caller : new ExpandedCaller(caller, null));

        } else {

            final ObjectSchema callerSchema = namespace.requireObjectSchema(CALLER_SCHEMA);

            final Set<Path> expand = new HashSet<>();
            if(permission != null) {
                final Set<Path> paths = Path.children(permission.getExpand(), Path.of(VAR_CALLER));
                // FIXME?
                expand.addAll(callerSchema.requiredExpand(paths));
            }

            if(caller instanceof ExpandedCaller) {

                final Instance object = ((ExpandedCaller)caller).getObject();
                return expand(Caller.SUPER, object, expand)
                        .thenApply(result -> result == object ? caller : new ExpandedCaller(caller, result));

            } else {

                return read(Caller.SUPER, caller.getSchema(), caller.getId(), new ReadOptions().setExpand(expand))
                        .thenApply(result -> new ExpandedCaller(caller, result));
            }
        }
    }

    private CompletableFuture<Instance> expand(final Caller caller, final Instance item, final Set<Path> expand) {

        if(item == null) {
            return CompletableFuture.completedFuture(null);
        } else if(expand == null || expand.isEmpty()) {
            return CompletableFuture.completedFuture(item);
        } else {
            final ExpandKey<RefKey> expandKey = ExpandKey.from(RefKey.from(item), expand);
            return expand(caller, Collections.singletonMap(expandKey, item))
                    .thenApply(results -> results.get(expandKey));
        }
    }

    private CompletableFuture<PagedList<Instance>> expand(final Caller caller, final PagedList<Instance> items, final Set<Path> expand) {

        if(items == null) {
            return CompletableFuture.completedFuture(null);
        } else if(expand == null || expand.isEmpty() || items.isEmpty()) {
            return CompletableFuture.completedFuture(items);
        } else {
            final Map<ExpandKey<RefKey>, Instance> expandKeys = items.stream()
                    .collect(Collectors.toMap(
                            item -> ExpandKey.from(RefKey.from(item), expand),
                            item -> item
                    ));
            return expand(caller, expandKeys)
                    .thenApply(expanded -> items.withPage(
                            items.stream()
                                    .map(v -> expanded.get(ExpandKey.from(RefKey.from(v), expand)))
                                    .collect(Collectors.toList())
                    ));
        }
    }

    private CompletableFuture<Map<ExpandKey<RefKey>, Instance>> expand(final Caller caller, final Map<ExpandKey<RefKey>, Instance> items) {

        final Set<ExpandKey<RefKey>> refs = new HashSet<>();
        final Map<ExpandKey<LinkKey>, CompletableFuture<PagedList<Instance>>> links = new HashMap<>();

        final Consistency consistency = Consistency.ATOMIC;

        items.forEach((ref, object) -> {
            if(!ref.getExpand().isEmpty()) {
                final ObjectSchema schema = namespace.requireObjectSchema(ref.getKey().getSchema());
                schema.expand(object, new Expander() {
                    @Override
                    public Instance ref(final ObjectSchema schema, final Instance ref, final Set<Path> expand) {

                        if(ref == null) {
                            return null;
                        } else {
                            refs.add(ExpandKey.from(RefKey.from(schema, ref), expand));
                            return ref;
                        }
                    }

                    @Override
                    public PagedList<Instance> link(final Link link, final PagedList<Instance> value, final Set<Path> expand) {

                        final RefKey refKey = ref.getKey();
                        final ExpandKey<LinkKey> linkKey = ExpandKey.from(LinkKey.from(refKey, link.getName()), expand);
                        log.debug("Expanding link: {}", linkKey);
                        links.put(linkKey, page(caller, refKey.getSchema(), refKey.getId(), link.getName(), new QueryOptions().setExpand(expand)));
                        return null;
                    }
                }, ref.getExpand());
            }
        });

        if(refs.isEmpty() && links.isEmpty()) {

            return CompletableFuture.completedFuture(items);

        } else {

            return CompletableFuture.allOf(links.values().toArray(new CompletableFuture<?>[0])).thenCompose(ignored -> {

                if(!refs.isEmpty()) {
                    log.debug("Expanding refs: {}", refs);
                }

                final Storage.ReadTransaction readTransaction = storage.read(consistency);
                refs.forEach(ref -> {
                    final RefKey refKey = ref.getKey();
                    final ObjectSchema objectSchema = schema(refKey.getSchema());
                    readTransaction.readObject(objectSchema, refKey.getId());
                });

                return readTransaction.read().thenCompose(results -> {

                    final Map<ExpandKey<RefKey>, Instance> resolved = new HashMap<>();
                    for (final Map<String, Object> initial : results.values()) {

                        final String schemaName = Instance.getSchema(initial);
                        final String id = Instance.getId(initial);
                        final ObjectSchema schema = schema(schemaName);
                        final Permission permission = schema.getPermission(Permission.READ);

                        final Instance object = schema.create(initial);

                        //FIXME: Need to expand for read permissions here (check for infinite recursion needed?)

                        checkPermission(caller, schema, permission, ImmutableMap.of(
                                VAR_THIS, object
                        ));

                        refs.forEach(ref -> {
                            final RefKey refKey = ref.getKey();
                            if (refKey.getId().equals(id)) {
                                resolved.put(ExpandKey.from(refKey, ref.getExpand()), object);
                            }
                        });
                    }

                    return cast(resolved).thenCompose(cast -> expand(caller, cast)).thenApply(expanded -> {

                        final Map<ExpandKey<RefKey>, Instance> result = new HashMap<>();

                        items.forEach((ref, object) -> {
                            final RefKey refKey = ref.getKey();
                            final ObjectSchema schema = namespace.requireObjectSchema(refKey.getSchema());

                            result.put(ref, schema.expand(object, new Expander() {
                                @Override
                                public Instance ref(final ObjectSchema schema, final Instance ref, final Set<Path> expand) {

                                    final ExpandKey<RefKey> expandKey = ExpandKey.from(RefKey.from(schema, ref), expand);
                                    Instance result = expanded.get(expandKey);
                                    if (result == null) {
                                        result = ObjectSchema.ref(Instance.getId(ref));
                                    }
                                    return result;
                                }

                                @Override
                                public PagedList<Instance> link(final Link link, final PagedList<Instance> value, final Set<Path> expand) {

                                    final ExpandKey<LinkKey> linkKey = ExpandKey.from(LinkKey.from(refKey, link.getName()), expand);
                                    final CompletableFuture<PagedList<Instance>> future = links.get(linkKey);
                                    if(future != null) {
                                        final PagedList<Instance> result = future.getNow(null);
                                        assert result != null;
                                        return result;
                                    } else {
                                        return null;
                                    }
                                }
                            }, ref.getExpand()));

                        });

                        return result;
                    });

                });
            });
        }
    }

    @Data
    private static class RefKey {

        private final String schema;

        private final String id;

        public static RefKey from(final Map<String, Object> item) {

            final String schema = Instance.getSchema(item);
            final String id = Instance.getId(item);
            assert schema != null && id != null;
            return new RefKey(schema, id);
        }

        public static RefKey from(final ObjectSchema schema, final Map<String, Object> item) {

            final String id = Instance.getId(item);
            assert id != null;
            return new RefKey(schema.getName(), id);
        }
    }

    @Data
    private static class LinkKey {

        private final String schema;

        private final String id;

        private final String link;

        public static LinkKey from(final Map<String, Object> item, final String link) {

            return new LinkKey(Instance.getSchema(item), Instance.getId(item), link);
        }

        public static LinkKey from(final RefKey ref, final String link) {

            return new LinkKey(ref.getSchema(), ref.getId(), link);
        }
    }

    @Data
    private static class ExpandKey<T> {

        private final T key;

        private final Set<Path> expand;

        public static <T> ExpandKey<T> from(final T key, final Set<Path> expand) {

            return new ExpandKey<>(key, expand);
        }
    }

    private static class ExpandedCaller extends Caller.Delegating {

        private final Instance object;

        public ExpandedCaller(final Caller caller, final Instance object) {

            super(caller);
            this.object = object != null ? object : getObject(caller);
        }

        public static Instance getObject(final Caller caller) {

            if(caller instanceof ExpandedCaller) {
                return ((ExpandedCaller)caller).getObject();
            } else {
                final HashMap<String, Object> object = new HashMap<>();
                object.put(Reserved.ID, caller.getId());
                object.put(Reserved.SCHEMA, CALLER_SCHEMA);
                // FIXME
                return new Instance(object);
            }
        }

        public Instance getObject() {

            return object;
        }
    }

//    private CompletableFuture<Collection<Instance>> readAbbreviated(final Caller caller, final ObjectSchema schema, final Collection<Map<String, Object>> object, final boolean abbreviated) {
//
//        if(abbreviated) {
//            final String id = ObjectSchema.getId(object);
//            final Long version = ObjectSchema.getVersion(object);
//            assert(id != null && version != null);
//            return storage.readObjectVersion(schema, id, version);
//        } else {
//            return CompletableFuture.completedFuture(schema.create(object));
//        }
//    }

    private ObjectSchema schema(final String schema) {

        return namespace.requireObjectSchema(schema);
    }

    private CompletableFuture<?> onObjectCreated(final ObjectCreatedEvent event) {

        final ObjectSchema schema = schema(event.getSchema());
        final StorageTraits traits = storage.storageTraits(schema);
        final String id = event.getId();
        final Map<String, Object> after = event.getAfter();
        final Set<Event> events = new HashSet<>();
        final History history = schema.getHistory();
        if(history.isEnabled() && history.getConsistency(traits.getHistoryConsistency()).isAsync()) {
            final Long historyVersion = Instance.getVersion(event.getAfter());
            assert historyVersion != null;
            events.add(AsyncHistoryCreatedEvent.of(schema.getName(), id, historyVersion, after));
        }
        events.addAll(schema.getAllIndexes().values().stream().flatMap(index -> {
            final Consistency best = traits.getIndexConsistency(index.isMultiValue());
            if(index.getConsistency(best).isAsync()) {
                final Map<Index.Key, Map<String, Object>> records = index.readValues(after);
                return records.entrySet().stream()
                        .map(e -> AsyncIndexCreatedEvent.of(schema.getName(), index.getName(), id, 0L, e.getKey(), e.getValue()));
            } else {
                return Stream.empty();
            }
        }).collect(Collectors.toSet()));
        return emitter.emit(events);
    }

    private CompletableFuture<?> onObjectUpdated(final ObjectUpdatedEvent event) {

        final ObjectSchema schema = schema(event.getSchema());
        final StorageTraits traits = storage.storageTraits(schema);
        final String id = event.getId();
        final long version = event.getVersion();
        final Map<String, Object> before = event.getBefore();
        final Map<String, Object> after = event.getAfter();
        final Set<Event> events = new HashSet<>();
        final History history = schema.getHistory();
        if(history.isEnabled() && history.getConsistency(traits.getHistoryConsistency()).isAsync()) {
            final Long historyVersion = Instance.getVersion(event.getAfter());
            assert historyVersion != null;
            events.add(AsyncHistoryCreatedEvent.of(schema.getName(), id, historyVersion, after));
        }
        events.addAll(schema.getAllIndexes().values().stream().flatMap(index -> {
            final Consistency best = traits.getIndexConsistency(index.isMultiValue());
            if(index.getConsistency(best).isAsync()) {
                final IndexRecordDiff diff = IndexRecordDiff.from(index.readValues(before), index.readValues(after));
                final Stream<Event> create = diff.getCreate().entrySet().stream()
                        .map(e -> AsyncIndexCreatedEvent.of(schema.getName(), index.getName(), id, version, e.getKey(), e.getValue()));
                final Stream<Event> update = diff.getUpdate().entrySet().stream()
                        .map(e -> AsyncIndexUpdatedEvent.of(schema.getName(), index.getName(), id, version, e.getKey(), e.getValue()));
                final Stream<Event> delete = diff.getDelete().stream()
                        .map(key-> AsyncIndexDeletedEvent.of(schema.getName(), index.getName(), id, version, key));
                return Stream.of(create, update, delete)
                        .flatMap(v -> v);
            } else {
                return Stream.empty();
            }
        }).collect(Collectors.toSet()));
        return emitter.emit(events);
    }

    private CompletableFuture<?> onObjectDeleted(final ObjectDeletedEvent event) {

        final ObjectSchema schema = schema(event.getSchema());
        final StorageTraits traits = storage.storageTraits(schema);
        final String id = event.getId();
        final long version = event.getVersion();
        final Map<String, Object> before = event.getBefore();
        return emitter.emit(schema.getAllIndexes().values().stream().flatMap(index -> {
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

    private CompletableFuture<?> onAsyncIndexCreated(final AsyncIndexCreatedEvent event) {

        final ObjectSchema schema = schema(event.getSchema());
        final Index index = schema.requireIndex(event.getIndex(), true);
        final Storage.WriteTransaction write = storage.write(Consistency.ASYNC);
        write.createIndex(schema, index, event.getId(), event.getVersion(), event.getKey(), event.getProjection());
        return write.commit();
    }

    private CompletableFuture<?> onAsyncIndexUpdated(final AsyncIndexUpdatedEvent event) {

        final ObjectSchema schema = schema(event.getSchema());
        final Index index = schema.requireIndex(event.getIndex(), true);
        final Storage.WriteTransaction write = storage.write(Consistency.ASYNC);
        write.updateIndex(schema, index, event.getId(), event.getVersion(), event.getKey(), event.getProjection());
        return write.commit();
    }

    private CompletableFuture<?> onAsyncIndexDeleted(final AsyncIndexDeletedEvent event) {

        final ObjectSchema schema = schema(event.getSchema());
        final Index index = schema.requireIndex(event.getIndex(), true);
        final Storage.WriteTransaction write = storage.write(Consistency.ASYNC);
        write.deleteIndex(schema, index, event.getId(), event.getVersion(), event.getKey());
        return write.commit();
    }

    private CompletableFuture<?> onAsyncHistoryCreated(final AsyncHistoryCreatedEvent event) {

        final ObjectSchema schema = schema(event.getSchema());
        final Storage.WriteTransaction write = storage.write(Consistency.ASYNC);
        write.createHistory(schema, event.getId(), event.getVersion(), event.getAfter());
        return write.commit();
    }

//    private CompletableFuture<?> onRefCreated(final RefCreatedEvent e) {
//
//        return CompletableFuture.completedFuture(null);
//    }
//
//    private CompletableFuture<?> onRefUpdated(final RefUpdatedEvent e) {
//
//        return CompletableFuture.completedFuture(null);
//    }
//
//    private CompletableFuture<?> onRefDeleted(final RefDeletedEvent e) {
//
//        return CompletableFuture.completedFuture(null);
//    }

}

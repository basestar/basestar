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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import io.basestar.auth.Caller;
import io.basestar.database.util.ExpandKey;
import io.basestar.database.util.QueryKey;
import io.basestar.database.util.RefKey;
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.schema.*;
import io.basestar.schema.util.Expander;
import io.basestar.storage.Storage;
import io.basestar.util.*;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Provides read implementation for database, does not apply permissions, the database needs to handle applying
 * permissions to returned data, and calculating the necessary expand sets to be able to apply permissions.
 */

@Slf4j
public class ReadProcessor {

    private static final int EXPAND_LINK_SIZE = 100;

    protected final Namespace namespace;

    protected final Storage storage;

    public ReadProcessor(final Namespace namespace, final Storage storage) {

        this.namespace = namespace;
        this.storage = storage;
    }

    protected ReferableSchema referableSchema(final Name schema) {

        return namespace.requireReferableSchema(schema);
    }

    protected LinkableSchema linkableSchema(final Name schema) {

        return namespace.requireLinkableSchema(schema);
    }

    protected QueryableSchema queryableSchema(final Name schema) {

        return namespace.requireQueryableSchema(schema);
    }

    protected CompletableFuture<Instance> readImpl(final ReferableSchema objectSchema, final String id, final Long version, final Set<Name> expand) {

        return readRaw(objectSchema, id, version, expand).thenApply(raw -> create(raw, expand));
    }

    private CompletableFuture<Map<String, Object>> readRaw(final ReferableSchema objectSchema, final String id, final Long version, final Set<Name> expand) {

        if (version == null) {
            return storage.get(consistency(objectSchema), objectSchema, id, expand);
        } else {
            return storage.get(consistency(objectSchema), objectSchema, id, expand)
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
                                return storage.getVersion(Consistency.ATOMIC, objectSchema, id, version, expand);
                            }
                        }
                    });
        }
    }

    private Consistency consistency(final ReferableSchema objectSchema) {

        return Consistency.ATOMIC;
    }

    protected CompletableFuture<Page<Instance>> queryLinkImpl(final Context context, final Consistency consistency, final Link link, final Instance owner,
                                                              final Set<Name> expand, final int count, final Page.Token paging, final Set<Page.Stat> stats) {

        final Expression expression = link.getExpression()
                .bind(context.with(ImmutableMap.of(
                        Reserved.THIS, owner
                )));

        final LinkableSchema linkSchema = link.getSchema();
        return queryImpl(context, consistency, linkSchema, Immutable.map(), expression, link.getSort(), expand, count, paging, stats);
    }

    protected CompletableFuture<Page<Instance>> queryImpl(final Context context, final Consistency consistency, final QueryableSchema schema, final Map<String, Object> arguments, final Expression expression,
                                                          final List<Sort> sort, final Set<Name> expand, final int count, final Page.Token paging, final Set<Page.Stat> stats) {

        final List<Sort> pageSort = schema.sort(sort);

        final Set<Name> queryExpand = Sets.union(Nullsafe.orDefault(expand), Nullsafe.orDefault(schema.getExpand()));

        if (!arguments.isEmpty()) {
            if (!(schema instanceof QuerySchema)) {
                throw new IllegalStateException("Arguments not supported for query on " + schema.getQualifiedName());
            }
        }

        final Pager<Instance> pager = storage.query(consistency, schema, arguments, expression, pageSort, queryExpand)
                .map(v -> create(v, expand));

        return pager.page(stats, paging, count)
                .thenApply(results -> {
                    // Expressions that could not be pushed down are applied after auto-paging.
                    return results.filter(instance -> {
                        try {
                            return expression.evaluatePredicate(context.with(instance));
                        } catch (final Exception e) {
                            // FIXME:
                            log.warn("Failed to evaluate predicate", e);
                            return false;
                        }
                    });
                });
    }

    protected RefKey latestRefKey(final Instance instance) {

        return latestRefKey(null, instance);
    }

    protected RefKey latestRefKey(final Name defaultSchema, final Instance instance) {

        assert instance != null;
        final Name schemaName = Nullsafe.orDefault(instance.getSchema(), defaultSchema);
        final LinkableSchema schema = linkableSchema(schemaName);
        assert schema != null;
        final String id = schema.forceId(instance);
        return new RefKey(schema.getQualifiedName(), id, null);

    }

    protected CompletableFuture<Instance> expand(final Consistency consistency, final Consistency linkConsistency, final Context context, final Instance item, final Set<Name> expand) {

        if (item == null) {
            return CompletableFuture.completedFuture(null);
        } else if (expand == null || expand.isEmpty()) {
            return CompletableFuture.completedFuture(item);
        } else {
            final ExpandKey<RefKey> expandKey = ExpandKey.from(latestRefKey(item), expand);
            return expand(consistency, linkConsistency, context, Collections.singletonMap(expandKey, item))
                    .thenApply(results -> results.get(expandKey));
        }
    }


    protected CompletableFuture<Page<Instance>> expand(final Consistency consistency, final Consistency linkConsistency, final Context context, final Page<Instance> items, final Set<Name> expand) {

        if(items == null) {
            return CompletableFuture.completedFuture(null);
        } else if(expand == null || expand.isEmpty() || items.isEmpty()) {
            return CompletableFuture.completedFuture(items);
        } else {
            final Map<ExpandKey<RefKey>, Instance> expandKeys = items.stream()
                    .collect(Collectors.toMap(
                            item -> ExpandKey.from(latestRefKey(item), expand),
                            item -> item
                    ));
            return expand(consistency, linkConsistency, context, expandKeys)
                    .thenApply(expanded -> items.withItems(
                            items.stream()
                                    .map(v -> expanded.get(ExpandKey.from(latestRefKey(v), expand)))
                                    .collect(Collectors.toList())
                    ));
        }
    }

    protected CompletableFuture<Map<ExpandKey<RefKey>, Instance>> expand(final Consistency consistency, final Consistency linkConsistency, final Context context, final Map<ExpandKey<RefKey>, Instance> items) {

        return expandImpl(consistency, linkConsistency, context, items)
                .thenApply(results -> {
                    final Map<ExpandKey<RefKey>, Instance> evaluated = new HashMap<>();
                    results.forEach((k, v) -> {
                        final LinkableSchema schema = linkableSchema(Instance.getSchema(v));
                        evaluated.put(k, schema.evaluateTransients(context, v, k.getExpand()));
                    });
                    return evaluated;
                });
    }

    // FIXME: poor performance, need to batch more
    protected CompletableFuture<Map<ExpandKey<RefKey>, Instance>> expandImpl(final Consistency consistency, final Consistency linkConsistency, final Context context, final Map<ExpandKey<RefKey>, Instance> items) {

        final Set<ExpandKey<RefKey>> refs = new HashSet<>();
        final Map<ExpandKey<QueryKey>, CompletableFuture<Page<Instance>>> links = new HashMap<>();

        items.forEach((ref, object) -> {
            if(!ref.getExpand().isEmpty()) {
                final Name baseSchemaName = ref.getKey().getSchema();
                final Name instanceSchemaName = Instance.getSchema(object);
                final LinkableSchema resolvedSchema = linkableSchema(Nullsafe.orDefault(instanceSchemaName, baseSchemaName));
                resolvedSchema.expand(object, new Expander() {
                    @Override
                    public Instance expandRef(final Name name, final ReferableSchema schema, final Instance ref, final Set<Name> expand) {

                        if(ref == null) {
                            return null;
                        } else {
                            refs.add(ExpandKey.from(latestRefKey(schema.getQualifiedName(), ref), expand));
                            return ref;
                        }
                    }

                    @Override
                    public Instance expandVersionedRef(final Name name, final ReferableSchema schema, final Instance ref, final Set<Name> expand) {

                        if(ref == null) {
                            return null;
                        } else {
                            refs.add(ExpandKey.from(RefKey.versioned(schema.getQualifiedName(), ref), expand));
                            return ref;
                        }
                    }

                    @Override
                    public Page<Instance> expandLink(final Name name, final Link link, final Page<Instance> value, final Set<Name> expand) {

                        final RefKey refKey = ref.getKey();
                        final ExpandKey<QueryKey> linkKey = ExpandKey.from(QueryKey.from(refKey, link.getName()), expand);
                        log.debug("Expanding link: {}", linkKey);
                        // FIXME: do we need to pre-expand here? original implementation did
                        links.put(linkKey, queryLinkImpl(context, linkConsistency, link, object, linkKey.getExpand(), EXPAND_LINK_SIZE, null, Collections.emptySet())
                                .thenCompose(results -> expand(consistency, linkConsistency, context, results, expand)));
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
                    final ReferableSchema schema = referableSchema(refKey.getSchema());
                    if (refKey.getVersion() != null) {
                        readTransaction.getVersion(schema, refKey.getId(), refKey.getVersion(), ref.getExpand());
                    } else {
                        readTransaction.get(schema, refKey.getId(), ref.getExpand());
                    }
                });

                return readTransaction.read().thenCompose(results -> {

                    final Map<ExpandKey<RefKey>, Instance> resolved = new HashMap<>();
                    for (final Map<String, Object> initial : results.getRefs().values()) {

                        final String id = Instance.getId(initial);
                        final Long version = Instance.getVersion(initial);
                        final Instance object = create(initial);

                        refs.forEach(ref -> {
                            final RefKey refKey = ref.getKey();
                            if (refKey.getId().equals(id)) {
                                if(refKey.getVersion() != null) {
                                    if (version.equals(refKey.getVersion())) {
                                        resolved.put(ExpandKey.from(refKey, ref.getExpand()), object);
                                    }
                                } else {
                                    // this is awkward, could rely on implicit ordering of BatchResult versions
                                    final ExpandKey<RefKey> expandKey = ExpandKey.from(refKey, ref.getExpand());
                                    final Instance prev = resolved.get(expandKey);
                                    if(prev == null || version > Instance.getVersion(prev)) {
                                        resolved.put(expandKey, object);
                                    }
                                }
                            }
                        });
                    }

                    return expand(consistency, linkConsistency, context, resolved).thenApply(expanded -> {

                        final Map<ExpandKey<RefKey>, Instance> result = new HashMap<>();

                        items.forEach((ref, object) -> {
                            final RefKey refKey = ref.getKey();
                            final Name baseSchemaName = ref.getKey().getSchema();
                            final Name instanceSchemaName = Instance.getSchema(object);
                            final LinkableSchema resolvedSchema = linkableSchema(Nullsafe.orDefault(instanceSchemaName, baseSchemaName));

                            result.put(ref, resolvedSchema.expand(object, new Expander() {
                                @Override
                                public Instance expandRef(final Name name, final ReferableSchema schema, final Instance ref, final Set<Name> expand) {

                                    final ExpandKey<RefKey> expandKey = ExpandKey.from(latestRefKey(schema.getQualifiedName(), ref), expand);
                                    Instance result = expanded.get(expandKey);
                                    if (result == null) {
                                        result = ReferableSchema.ref(Instance.getId(ref));
                                    }
                                    return result;
                                }

                                @Override
                                public Instance expandVersionedRef(final Name name, final ReferableSchema schema, final Instance ref, final Set<Name> expand) {

                                    final ExpandKey<RefKey> expandKey = ExpandKey.from(RefKey.versioned(schema.getQualifiedName(), ref), expand);
                                    Instance result = expanded.get(expandKey);
                                    if (result == null) {
                                        result = ReferableSchema.versionedRef(Instance.getId(ref), Instance.getVersion(ref));
                                    }
                                    return result;
                                }

                                @Override
                                public Page<Instance> expandLink(final Name name, final Link link, final Page<Instance> value, final Set<Name> expand) {

                                    final ExpandKey<QueryKey> linkKey = ExpandKey.from(QueryKey.from(refKey, link.getName()), expand);
                                    final CompletableFuture<Page<Instance>> future = links.get(linkKey);
                                    if(future != null) {
                                        final Page<Instance> result = future.getNow(null);
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

    protected Instance create(final Map<String, Object> data) {

        if(data == null) {
            return null;
        }
        final QueryableSchema schema = queryableSchema(Instance.getSchema(data));
        return schema.create(data, schema.getExpand(), true);
    }

    protected Instance create(final Map<String, Object> data, final Set<Name> expand) {

        if(data == null) {
            return null;
        }
        final QueryableSchema schema = queryableSchema(Instance.getSchema(data));
        return schema.create(data, Immutable.addAll(schema.getExpand(), expand), true);
    }

    protected CompletableFuture<Caller> expandCaller(final Consistency consistency, final Consistency linkConsistency, final Context context, final Caller caller, final Set<Name> expand) {

        if(caller.getId() == null || caller.isSuper()) {

            return CompletableFuture.completedFuture(caller instanceof ExpandedCaller ? caller : new ExpandedCaller(caller, null));

        } else {

            if(caller instanceof ExpandedCaller) {

                final Instance object = ((ExpandedCaller)caller).getObject();
                return expand(consistency, linkConsistency, context, object, expand)
                        .thenApply(result -> result == object ? caller : new ExpandedCaller(caller, result));

            } else {

                if(caller.getSchema() != null) {
                    final Schema schema = namespace.getSchema(caller.getSchema());
                    if(schema instanceof ObjectSchema) {
                        return readImpl((ObjectSchema)schema, caller.getId(), null, expand)
                                .thenCompose(unexpanded -> expand(consistency, linkConsistency, context, unexpanded, expand))
                                .thenApply(result -> new ExpandedCaller(caller, result));
                    }
                }
                return CompletableFuture.completedFuture(new ExpandedCaller(caller, null));
            }
        }
    }

    protected static class ExpandedCaller extends Caller.Delegating {

        private static final long serialVersionUID = 1L;

        private final Instance object;

        public ExpandedCaller(final Caller caller, final Instance object) {

            super(caller);
            this.object = object != null ? object : getObject(caller);
        }

        public static Instance getObject(final Caller caller) {

            if(caller instanceof ExpandedCaller) {
                final Map<String, Object> object = ((ExpandedCaller)caller).getObject();
                if(object != null) {
                    return new Instance(object);
                }
            }
            final HashMap<String, Object> object = new HashMap<>();
            object.put(ObjectSchema.ID, caller.getId());
            object.put(ObjectSchema.SCHEMA, caller.getSchema());
            return new Instance(object);
        }

        public Instance getObject() {

            return object;
        }
    }
}

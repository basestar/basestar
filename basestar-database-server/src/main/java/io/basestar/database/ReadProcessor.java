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

import com.google.common.collect.*;
import io.basestar.auth.Caller;
import io.basestar.database.util.ExpandKey;
import io.basestar.database.util.LinkKey;
import io.basestar.database.util.RefKey;
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.schema.*;
import io.basestar.schema.util.Expander;
import io.basestar.storage.Storage;
import io.basestar.util.*;
import lombok.Data;
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

    public ReadProcessor(Namespace namespace, Storage storage) {
        this.namespace = namespace;
        this.storage = storage;
    }

    protected ObjectSchema objectSchema(final Name schema) {

        return namespace.requireObjectSchema(schema);
    }

    protected CompletableFuture<Instance> readImpl(final ObjectSchema objectSchema, final String id, final Long version, final Set<Name> expand) {

        // Will make 2 reads if the request schema doesn't match result schema

        return readRaw(objectSchema, id, version, expand).thenCompose(raw -> cast(objectSchema, raw, expand));
    }

    private CompletableFuture<Map<String, Object>> readRaw(final ObjectSchema objectSchema, final String id, final Long version, final Set<Name> expand) {

        if (version == null) {
            return storage.readObject(objectSchema, id, expand);
        } else {
            return storage.readObject(objectSchema, id, expand)
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
                                return storage.readObjectVersion(objectSchema, id, version, expand);
                            }
                        }
                    });
        }
    }

    protected CompletableFuture<Page<Instance>> queryLinkImpl(final Context context, final Link link, final Instance owner,
                                                              final Set<Name> expand, final int count, final Page.Token paging) {

        final Expression expression = link.getExpression()
                .bind(context.with(ImmutableMap.of(
                        Reserved.THIS, owner
                )));

        //FIXME: must support views
        final ObjectSchema linkSchema = (ObjectSchema)link.getSchema();
        return queryImpl(context, linkSchema, expression, link.getSort(), expand, count, paging);
    }

    protected CompletableFuture<Page<Instance>> queryImpl(final Context context, final ObjectSchema objectSchema, final Expression expression,
                                                          final List<Sort> sort, final Set<Name> expand, final int count, final Page.Token paging) {

        final List<Sort> pageSort = ImmutableList.<Sort>builder()
                .addAll(sort)
                .add(Sort.asc(Name.of(ObjectSchema.ID)))
                .build();

        final Set<Name> queryExpand = Sets.union(Nullsafe.orDefault(expand), Nullsafe.orDefault(objectSchema.getExpand()));

        final List<Pager.Source<Instance>> sources = storage.query(objectSchema, expression, sort, queryExpand).stream()
                .map(source -> (Pager.Source<Instance>) (c, t, stats) -> source.page(c, t, stats)
                        .thenCompose(data -> cast(objectSchema, data, queryExpand)))
                .collect(Collectors.toList());

        return pageImpl(context, sources, expression, pageSort, count, paging);
    }

    protected CompletableFuture<Page<Instance>> pageImpl(final Context context, final List<Pager.Source<Instance>> sources, final Expression expression,
                                                         final List<Sort> sort, final int count, final Page.Token paging) {

        if(sources.isEmpty()) {
            throw new IllegalStateException("Query not supported");
        } else {

            final Comparator<Map<String, Object>> comparator = Instance.comparator(sort);
            final Pager<Instance> pager = new Pager<>(comparator, sources, paging);
            return pager.page(count)
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
    }

    protected CompletableFuture<Instance> expand(final Context context, final Instance item, final Set<Name> expand) {

        if(item == null) {
            return CompletableFuture.completedFuture(null);
        } else if(expand == null || expand.isEmpty()) {
            return CompletableFuture.completedFuture(item);
        } else {
            final ExpandKey<RefKey> expandKey = ExpandKey.from(RefKey.latest(item), expand);
            return expand(context, Collections.singletonMap(expandKey, item))
                    .thenApply(results -> results.get(expandKey));
        }
    }

    protected CompletableFuture<Page<Instance>> expand(final Context context, final Page<Instance> items, final Set<Name> expand) {

        if(items == null) {
            return CompletableFuture.completedFuture(null);
        } else if(expand == null || expand.isEmpty() || items.isEmpty()) {
            return CompletableFuture.completedFuture(items);
        } else {
            final Map<ExpandKey<RefKey>, Instance> expandKeys = items.stream()
                    .collect(Collectors.toMap(
                            item -> ExpandKey.from(RefKey.latest(item), expand),
                            item -> item
                    ));
            return expand(context, expandKeys)
                    .thenApply(expanded -> items.withPage(
                            items.stream()
                                    .map(v -> expanded.get(ExpandKey.from(RefKey.latest(v), expand)))
                                    .collect(Collectors.toList())
                    ));
        }
    }

    protected CompletableFuture<Map<ExpandKey<RefKey>, Instance>> expand(final Context context, final Map<ExpandKey<RefKey>, Instance> items) {

        return expandImpl(context, items)
                .thenApply(results -> {
                    final Map<ExpandKey<RefKey>, Instance> evaluated = new HashMap<>();
                    results.forEach((k, v) -> {
                        final ObjectSchema schema = objectSchema(Instance.getSchema(v));
                        evaluated.put(k, schema.evaluateTransients(context, v, k.getExpand()));
                    });
                    return evaluated;
                });
    }

    // FIXME: poor performance, need to batch more
    protected CompletableFuture<Map<ExpandKey<RefKey>, Instance>> expandImpl(final Context context, final Map<ExpandKey<RefKey>, Instance> items) {

        final Set<ExpandKey<RefKey>> refs = new HashSet<>();
        final Map<ExpandKey<LinkKey>, CompletableFuture<Page<Instance>>> links = new HashMap<>();

        final Consistency consistency = Consistency.ATOMIC;

        items.forEach((ref, object) -> {
            if(!ref.getExpand().isEmpty()) {
                final Name baseSchemaName = ref.getKey().getSchema();
                final Name instanceSchemaName = Instance.getSchema(object);
                final ObjectSchema resolvedSchema = objectSchema(Nullsafe.orDefault(instanceSchemaName, baseSchemaName));
                resolvedSchema.expand(object, new Expander() {
                    @Override
                    public Instance expandRef(final ObjectSchema schema, final Instance ref, final Set<Name> expand) {

                        if(ref == null) {
                            return null;
                        } else {
                            refs.add(ExpandKey.from(RefKey.latest(schema.getQualifiedName(), ref), expand));
                            return ref;
                        }
                    }

                    @Override
                    public Instance expandVersionedRef(final ObjectSchema schema, final Instance ref, final Set<Name> expand) {

                        if(ref == null) {
                            return null;
                        } else {
                            refs.add(ExpandKey.from(RefKey.versioned(schema.getQualifiedName(), ref), expand));
                            return ref;
                        }
                    }

                    @Override
                    public Page<Instance> expandLink(final Link link, final Page<Instance> value, final Set<Name> expand) {

                        final RefKey refKey = ref.getKey();
                        final ExpandKey<LinkKey> linkKey = ExpandKey.from(LinkKey.from(refKey, link.getName()), expand);
                        log.debug("Expanding link: {}", linkKey);
                        // FIXME: do we need to pre-expand here? original implementation did
                        links.put(linkKey, queryLinkImpl(context, link, object, linkKey.getExpand(), EXPAND_LINK_SIZE, null)
                                .thenCompose(results -> expand(context, results, expand)));
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
                    final ObjectSchema objectSchema = objectSchema(refKey.getSchema());
                    if(refKey.getVersion() != null) {
                        readTransaction.readObjectVersion(objectSchema, refKey.getId(), refKey.getVersion(), ref.getExpand());
                    } else {
                        readTransaction.readObject(objectSchema, refKey.getId(), ref.getExpand());
                    }
                });

                return readTransaction.read().thenCompose(results -> {

                    final Map<ExpandKey<RefKey>, Instance> resolved = new HashMap<>();
                    for (final Map<String, Object> initial : results.values()) {

                        final Name schemaName = Instance.getSchema(initial);
                        final String id = Instance.getId(initial);
                        final Long version = Instance.getVersion(initial);
                        final ObjectSchema schema = objectSchema(schemaName);
                        final Instance object = create(schema, initial);

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

                    return cast(resolved).thenCompose(v -> expand(context, v)).thenApply(expanded -> {

                        final Map<ExpandKey<RefKey>, Instance> result = new HashMap<>();

                        items.forEach((ref, object) -> {
                            final RefKey refKey = ref.getKey();
                            final Name baseSchemaName = ref.getKey().getSchema();
                            final Name instanceSchemaName = Instance.getSchema(object);
                            final ObjectSchema resolvedSchema = objectSchema(Nullsafe.orDefault(instanceSchemaName, baseSchemaName));

                            result.put(ref, resolvedSchema.expand(object, new Expander() {
                                @Override
                                public Instance expandRef(final ObjectSchema schema, final Instance ref, final Set<Name> expand) {

                                    final ExpandKey<RefKey> expandKey = ExpandKey.from(RefKey.latest(schema.getQualifiedName(), ref), expand);
                                    Instance result = expanded.get(expandKey);
                                    if (result == null) {
                                        result = ObjectSchema.ref(Instance.getId(ref));
                                    }
                                    return result;
                                }

                                @Override
                                public Instance expandVersionedRef(final ObjectSchema schema, final Instance ref, final Set<Name> expand) {

                                    final ExpandKey<RefKey> expandKey = ExpandKey.from(RefKey.versioned(schema.getQualifiedName(), ref), expand);
                                    Instance result = expanded.get(expandKey);
                                    if (result == null) {
                                        result = ObjectSchema.versionedRef(Instance.getId(ref), Instance.getVersion(ref));
                                    }
                                    return result;
                                }

                                @Override
                                public Page<Instance> expandLink(final Link link, final Page<Instance> value, final Set<Name> expand) {

                                    final ExpandKey<LinkKey> linkKey = ExpandKey.from(LinkKey.from(refKey, link.getName()), expand);
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

    protected CompletableFuture<Instance> cast(final ObjectSchema baseSchema, final Map<String, Object> data, final Set<Name> expand) {

        if(data == null) {
            return CompletableFuture.completedFuture(null);
        }
        final Name castSchemaName = Instance.getSchema(data);
        if(baseSchema.getQualifiedName().equals(castSchemaName)) {
            return CompletableFuture.completedFuture(create(baseSchema, data));
        } else {
            final String id = Instance.getId(data);
            final Long version = Instance.getVersion(data);
            final ObjectSchema castSchema = objectSchema(castSchemaName);
            return readRaw(castSchema, id, version, expand)
                    .thenApply(v -> create(castSchema, v));
        }
    }

    protected Instance create(final InstanceSchema schema, final Map<String, Object> data) {

        return schema.create(data, Collections.emptySet(), true);
    }

    protected CompletableFuture<Page<Instance>> cast(final ObjectSchema baseSchema, final Page<? extends Map<String, Object>> data, final Set<Name> expand) {

        final Multimap<Name, Map<String, Object>> needed = ArrayListMultimap.create();
        data.forEach(v -> {
            final Name actualSchema = Instance.getSchema(v);
            if(!baseSchema.getQualifiedName().equals(actualSchema)) {
                needed.put(actualSchema, v);
            }
        });
        if(needed.isEmpty()) {
            return CompletableFuture.completedFuture(data.map(v -> {
                final ObjectSchema schema = objectSchema(Instance.getSchema(v));
                return schema.create(v, expand, true);
            }));
        } else {
            final Storage.ReadTransaction readTransaction = storage.read(Consistency.NONE);
            needed.asMap().forEach((schemaName, items) -> {
                final ObjectSchema schema = objectSchema(schemaName);
                items.forEach(item -> {
                    final String id = Instance.getId(item);
                    final Long version = Instance.getVersion(item);
                    assert version != null;
                    readTransaction.readObjectVersion(schema, id, version, expand);
                });
            });
            return readTransaction.read().thenApply(results -> data.map(v -> {
                final Name schemaName = Instance.getSchema(v);
                final String id = Instance.getId(v);
                final Map<String, Object> mappedResult = results.getObject(schemaName, id);
                final Map<String, Object> result = Nullsafe.orDefault(mappedResult, v);
                final ObjectSchema schema = objectSchema(Instance.getSchema(result));
                return schema.create(result, expand, true);
            }));
        }
    }

    @Data
    private static class InstanceExpand {

        private final Map<String, Object> instance;

        private final Set<Name> expand;
    }

    protected CompletableFuture<Map<ExpandKey<RefKey>, Instance>> cast(final Map<ExpandKey<RefKey>, ? extends Map<String, Object>> data) {

        final Multimap<Name, InstanceExpand> needed = ArrayListMultimap.create();
        data.forEach((k, v) -> {
            final Name actualSchema = Instance.getSchema(v);
            if(!k.getKey().getSchema().equals(actualSchema)) {
                needed.put(actualSchema, new InstanceExpand(v, k.getExpand()));
            }
        });
        if(needed.isEmpty()) {
            final Map<ExpandKey<RefKey>, Instance> results = new HashMap<>();
            data.forEach((k, v) -> {
                final ObjectSchema schema = objectSchema(Instance.getSchema(v));
                final Instance instance = create(schema, v);
                results.put(k, instance);
            });
            return CompletableFuture.completedFuture(results);
        } else {
            final Storage.ReadTransaction readTransaction = storage.read(Consistency.NONE);
            needed.asMap().forEach((schemaName, items) -> {
                final ObjectSchema schema = objectSchema(schemaName);
                items.forEach(item -> {
                    final Map<String, Object> instance = item.getInstance();
                    final Set<Name> expand = item.getExpand();
                    final String id = Instance.getId(instance);
                    final Long version = Instance.getVersion(instance);
                    assert version != null;
                    readTransaction.readObjectVersion(schema, id, version, expand);
                });
            });
            return readTransaction.read().thenApply(results -> {

                final Map<ExpandKey<RefKey>, Instance> remapped = new HashMap<>();
                data.forEach((k, v) ->  {
                    final Name schemaName = Instance.getSchema(v);
                    final String id = Instance.getId(v);
                    final Long version = Instance.getVersion(v);
                    final Map<String, Object> mappedResult = results.getObjectVersion(schemaName, id, version);
                    final Map<String, Object> result = Nullsafe.orDefault(mappedResult, v);
                    final ObjectSchema schema = objectSchema(Instance.getSchema(result));
                    final Instance instance = create(schema, result);
                    remapped.put(k, instance);
                });
                return remapped;
            });
        }
    }

    protected CompletableFuture<Caller> expandCaller(final Context context, final Caller caller, final Set<Name> expand) {

        if(caller.getId() == null || caller.isSuper()) {

            return CompletableFuture.completedFuture(caller instanceof ExpandedCaller ? caller : new ExpandedCaller(caller, null));

        } else {

            if(caller instanceof ExpandedCaller) {

                final Instance object = ((ExpandedCaller)caller).getObject();
                return expand(context, object, expand)
                        .thenApply(result -> result == object ? caller : new ExpandedCaller(caller, result));

            } else {

                if(caller.getSchema() != null) {
                    final Schema<?> schema = namespace.getSchema(caller.getSchema());
                    if(schema instanceof ObjectSchema) {
                        return readImpl((ObjectSchema)schema, caller.getId(), null, expand)
                                .thenCompose(unexpanded -> expand(context, unexpanded, expand))
                                .thenApply(result -> new ExpandedCaller(caller, result));
                    }
                }
                return CompletableFuture.completedFuture(new ExpandedCaller(caller, null));
            }
        }
    }

    protected static class ExpandedCaller extends Caller.Delegating {

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

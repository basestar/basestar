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
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import io.basestar.auth.Caller;
import io.basestar.database.util.ExpandKey;
import io.basestar.database.util.LinkKey;
import io.basestar.database.util.RefKey;
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.schema.*;
import io.basestar.storage.Storage;
import io.basestar.storage.util.Pager;
import io.basestar.util.PagedList;
import io.basestar.util.PagingToken;
import io.basestar.util.Path;
import io.basestar.util.Sort;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Provides read implementation for database, does not apply permissions, the database needs to handle applying
 * permissions to returned data, and calculating the necessary expand sets to be able to apply permissions.
 */

@Slf4j
@RequiredArgsConstructor
public class ReadProcessor {

    private static final int EXPAND_LINK_SIZE = 100;

    protected final Namespace namespace;

    protected final Storage storage;

    protected ObjectSchema objectSchema(final String schema) {

        return namespace.requireObjectSchema(schema);
    }

    protected CompletableFuture<Instance> readImpl(final ObjectSchema objectSchema, final String id, final Long version) {

        // Will make 2 reads if the request schema doesn't match result schema

        return readRaw(objectSchema, id, version).thenCompose(raw -> cast(objectSchema, raw));
    }

    private CompletableFuture<Map<String, Object>> readRaw(final ObjectSchema objectSchema, final String id, final Long version) {

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

    protected CompletableFuture<PagedList<Instance>> queryLinkImpl(final Link link, final Instance owner,
                                                                   final int count, final PagingToken paging) {

        final Expression expression = link.getExpression()
                .bind(Context.init(ImmutableMap.of(
                        Reserved.THIS, owner
                )));

        final ObjectSchema linkSchema = link.getSchema();
        return queryImpl(linkSchema, expression, link.getSort(), count, paging);
    }

    protected CompletableFuture<PagedList<Instance>> queryImpl(final ObjectSchema objectSchema, final Expression expression,
                                                               final List<Sort> sort, final int count, final PagingToken paging) {

        final Context context = Context.init();

        final List<Sort> pageSort = ImmutableList.<Sort>builder()
                .addAll(sort)
                .add(Sort.asc(Path.of(Reserved.ID)))
                .build();

        final List<Pager.Source<Instance>> sources = storage.query(objectSchema, expression, sort).stream()
                .map(source -> (Pager.Source<Instance>) (c, t) -> source.page(c, t)
                        .thenCompose(data -> cast(objectSchema, data)))
                .collect(Collectors.toList());

        if(sources.isEmpty()) {
            throw new IllegalStateException("Query not supported");
        } else {

            @SuppressWarnings("unchecked")
            final Comparator<Instance> comparator = Sort.comparator(pageSort, (t, path) -> (Comparable)path.apply(t));
            final Pager<Instance> pager = new Pager<>(comparator, sources, paging);
            return pager.page(count)
                    .thenApply(results -> {
                        // Expressions that could not be pushed down are applied after auto-paging.
                        return results.filter(instance -> expression.evaluatePredicate(context.with(instance)));
                    });
        }
    }

    protected CompletableFuture<Instance> expand(final Instance item, final Set<Path> expand) {

        if(item == null) {
            return CompletableFuture.completedFuture(null);
        } else if(expand == null || expand.isEmpty()) {
            return CompletableFuture.completedFuture(item);
        } else {
            final ExpandKey<RefKey> expandKey = ExpandKey.from(RefKey.from(item), expand);
            return expand(Collections.singletonMap(expandKey, item))
                    .thenApply(results -> results.get(expandKey));
        }
    }

    protected CompletableFuture<PagedList<Instance>> expand(final PagedList<Instance> items, final Set<Path> expand) {

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
            return expand(expandKeys)
                    .thenApply(expanded -> items.withPage(
                            items.stream()
                                    .map(v -> expanded.get(ExpandKey.from(RefKey.from(v), expand)))
                                    .collect(Collectors.toList())
                    ));
        }
    }

    protected CompletableFuture<Map<ExpandKey<RefKey>, Instance>> expand(final Map<ExpandKey<RefKey>, Instance> items) {

        final Set<ExpandKey<RefKey>> refs = new HashSet<>();
        final Map<ExpandKey<LinkKey>, CompletableFuture<PagedList<Instance>>> links = new HashMap<>();

        final Consistency consistency = Consistency.ATOMIC;

        items.forEach((ref, object) -> {
            if(!ref.getExpand().isEmpty()) {
                final ObjectSchema schema = objectSchema(ref.getKey().getSchema());
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
                        // FIXME: do we need to pre-expand here? original implementation did
                        links.put(linkKey, queryLinkImpl(link, object, EXPAND_LINK_SIZE, null));
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
                    readTransaction.readObject(objectSchema, refKey.getId());
                });

                return readTransaction.read().thenCompose(results -> {

                    final Map<ExpandKey<RefKey>, Instance> resolved = new HashMap<>();
                    for (final Map<String, Object> initial : results.values()) {

                        final String schemaName = Instance.getSchema(initial);
                        final String id = Instance.getId(initial);
                        final ObjectSchema schema = objectSchema(schemaName);
                        final Instance object = schema.create(initial);

                        refs.forEach(ref -> {
                            final RefKey refKey = ref.getKey();
                            if (refKey.getId().equals(id)) {
                                resolved.put(ExpandKey.from(refKey, ref.getExpand()), object);
                            }
                        });
                    }

                    return cast(resolved).thenCompose(this::expand).thenApply(expanded -> {

                        final Map<ExpandKey<RefKey>, Instance> result = new HashMap<>();

                        items.forEach((ref, object) -> {
                            final RefKey refKey = ref.getKey();
                            final ObjectSchema schema = objectSchema(refKey.getSchema());

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

    protected CompletableFuture<Instance> cast(final ObjectSchema baseSchema, final Map<String, Object> data) {

        if(data == null) {
            return CompletableFuture.completedFuture(null);
        }
        final String castSchemaName = Instance.getSchema(data);
        if(baseSchema.getName().equals(castSchemaName)) {
            return CompletableFuture.completedFuture(baseSchema.create(data));
        } else {
            final String id = Instance.getId(data);
            final Long version = Instance.getVersion(data);
            final ObjectSchema castSchema = objectSchema(castSchemaName);
            return readRaw(castSchema, id, version)
                    .thenApply(castSchema::create);
        }
    }

    protected CompletableFuture<PagedList<Instance>> cast(final ObjectSchema baseSchema, final PagedList<? extends Map<String, Object>> data) {

        final Multimap<String, Map<String, Object>> needed = ArrayListMultimap.create();
        data.forEach(v -> {
            final String actualSchema = Instance.getSchema(v);
            if(!baseSchema.getName().equals(actualSchema)) {
                needed.put(actualSchema, v);
            }
        });
        if(needed.isEmpty()) {
            return CompletableFuture.completedFuture(data.map(v -> {
                final ObjectSchema schema = objectSchema(Instance.getSchema(v));
                return schema.create(v);
            }));
        } else {
            final Storage.ReadTransaction readTransaction = storage.read(Consistency.NONE);
            needed.asMap().forEach((schemaName, items) -> {
                final ObjectSchema schema = objectSchema(schemaName);
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
                    final ObjectSchema schema = objectSchema(Instance.getSchema(result));
                    return schema.create(result);
                });
            });
        }
    }

    protected CompletableFuture<Map<ExpandKey<RefKey>, Instance>> cast(final Map<ExpandKey<RefKey>, ? extends Map<String, Object>> data) {

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
                final ObjectSchema schema = objectSchema(Instance.getSchema(v));
                final Instance instance = schema.create(v);
                results.put(k, instance);
            });
            return CompletableFuture.completedFuture(results);
        } else {
            final Storage.ReadTransaction readTransaction = storage.read(Consistency.NONE);
            needed.asMap().forEach((schemaName, items) -> {
                final ObjectSchema schema = objectSchema(schemaName);
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
                    final ObjectSchema schema = objectSchema(Instance.getSchema(result));
                    final Instance instance = schema.create(result);
                    remapped.put(k, instance);
                });
                return remapped;
            });
        }
    }

    protected CompletableFuture<Caller> expandCaller(final Caller caller, final Set<Path> expand) {

        if(caller.getId() == null || caller.isSuper()) {

            return CompletableFuture.completedFuture(caller instanceof ExpandedCaller ? caller : new ExpandedCaller(caller, null));

        } else {

            if(caller instanceof ExpandedCaller) {

                final Instance object = ((ExpandedCaller)caller).getObject();
                return expand(object, expand)
                        .thenApply(result -> result == object ? caller : new ExpandedCaller(caller, result));

            } else {

                final ObjectSchema schema = objectSchema(caller.getSchema());
                return readImpl(schema, caller.getId(), null)
                        .thenCompose(unexpanded -> expand(unexpanded, expand))
                        .thenApply(result -> new ExpandedCaller(caller, result));
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
                return ((ExpandedCaller)caller).getObject();
            } else {
                final HashMap<String, Object> object = new HashMap<>();
                object.put(Reserved.ID, caller.getId());
                object.put(Reserved.SCHEMA, caller.getSchema());
                return new Instance(object);
            }
        }

        public Instance getObject() {

            return object;
        }
    }
}

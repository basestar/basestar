//package io.basestar.database;

/*-
 * #%L
 * basestar-database-server
 * $Id:$
 * $HeadURL:$
 * %%
 * Copyright (C) 2020 basestar.io
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
//
//import com.google.common.collect.ImmutableMap;
//import io.basestar.auth.Caller;
//import io.basestar.database.options.QueryOptions;
//import io.basestar.schema.*;
//import io.basestar.storage.Storage;
//import io.basestar.util.PagedList;
//import io.basestar.util.Path;
//import lombok.Data;
//import lombok.extern.slf4j.Slf4j;
//
//import java.util.*;
//import java.util.concurrent.CompletableFuture;
//import java.util.stream.Collectors;
//
//@Slf4j
//public class ExternalExpand {
//
//    private CompletableFuture<Instance> expand(final Caller caller, final Instance item, final Set<Path> expand) {
//
//        if(item == null) {
//            return CompletableFuture.completedFuture(null);
//        } else if(expand == null || expand.isEmpty()) {
//            return CompletableFuture.completedFuture(item);
//        } else {
//            final ExpandKey<RefKey> expandKey = ExpandKey.from(RefKey.from(item), expand);
//            return expand(caller, Collections.singletonMap(expandKey, item))
//                    .thenApply(results -> results.get(expandKey));
//        }
//    }
//
//    private CompletableFuture<PagedList<Instance>> expand(final Caller caller, final PagedList<Instance> items, final Set<Path> expand) {
//
//        if(items == null) {
//            return CompletableFuture.completedFuture(null);
//        } else if(expand == null || expand.isEmpty() || items.isEmpty()) {
//            return CompletableFuture.completedFuture(items);
//        } else {
//            final Map<ExpandKey<RefKey>, Instance> expandKeys = items.stream()
//                    .collect(Collectors.toMap(
//                            item -> ExpandKey.from(RefKey.from(item), expand),
//                            item -> item
//                    ));
//            return expand(caller, expandKeys)
//                    .thenApply(expanded -> items.withPage(
//                            items.stream()
//                                    .map(v -> expanded.get(ExpandKey.from(RefKey.from(v), expand)))
//                                    .collect(Collectors.toList())
//                    ));
//        }
//    }
//
//    private CompletableFuture<Map<ExpandKey<RefKey>, Instance>> expand(final Caller caller, final Map<ExpandKey<RefKey>, Instance> items) {
//
//        final Set<ExpandKey<RefKey>> refs = new HashSet<>();
//        final Map<ExpandKey<LinkKey>, CompletableFuture<PagedList<Instance>>> links = new HashMap<>();
//
//        final Consistency consistency = Consistency.ATOMIC;
//
//        items.forEach((ref, object) -> {
//            if(!ref.getExpand().isEmpty()) {
//                final ObjectSchema schema = namespace.requireObjectSchema(ref.getKey().getSchema());
//                schema.expand(object, new Expander() {
//                    @Override
//                    public Instance ref(final Instance ref, final Set<Path> expand) {
//
//                        if(ref == null) {
//                            return null;
//                        } else {
//                            refs.add(ExpandKey.from(RefKey.from(ref), expand));
//                            return ref;
//                        }
//                    }
//
//                    @Override
//                    public PagedList<Instance> link(final String link, final Set<Path> expand) {
//
//                        final RefKey refKey = ref.getKey();
//                        final ExpandKey<LinkKey> linkKey = ExpandKey.from(LinkKey.from(refKey, link), expand);
//                        log.debug("Expanding link: {}", linkKey);
//                        links.put(linkKey, page(caller, refKey.getSchema(), refKey.getId(), link, new QueryOptions().setExpand(expand)));
//                        return null;
//                    }
//                }, ref.getExpand());
//            }
//        });
//
//        if(refs.isEmpty() && links.isEmpty()) {
//
//            return CompletableFuture.completedFuture(items);
//
//        } else {
//
//            return CompletableFuture.allOf(links.values().toArray(new CompletableFuture<?>[0])).thenCompose(expandedLinks -> {
//
//                if(!refs.isEmpty()) {
//                    log.debug("Expanding refs: {}", refs);
//                }
//
//                final Storage.ReadTransaction readTransaction = storage.read(consistency);
//                refs.forEach(ref -> {
//                    final RefKey refKey = ref.getKey();
//                    final ObjectSchema objectSchema = namespace.requireObjectSchema(refKey.getSchema());
//                    readTransaction.readObject(objectSchema, refKey.getId());
//                });
//
//                return readTransaction.read().thenCompose(results -> {
//
//                    final Map<ExpandKey<RefKey>, Instance> resolved = new HashMap<>();
//                    for (final Map<String, Object> initial : results) {
//
//                        final String schemaName = Instance.getSchema(initial);
//                        final String id = Instance.getId(initial);
//                        final ObjectSchema schema = namespace.requireObjectSchema(schemaName);
//                        final Permission permission = schema.getPermission(Permission.READ);
//
//                        final Instance object = schema.create(initial);
//
//                        //FIXME: Need to expand for read permissions here (check for infinite recursion needed?)
//
//                        checkPermission(caller, schema, permission, ImmutableMap.of(
//                                VAR_THIS, object
//                        ));
//
//                        refs.forEach(ref -> {
//                            final RefKey refKey = ref.getKey();
//                            if (refKey.getId().equals(id)) {
//                                resolved.put(ExpandKey.from(refKey, ref.getExpand()), object);
//                            }
//                        });
//                    }
//
//                    return expand(caller, resolved).thenApply(expanded -> {
//
//                        final Map<ExpandKey<RefKey>, Instance> result = new HashMap<>();
//
//                        items.forEach((ref, object) -> {
//                            final RefKey refKey = ref.getKey();
//                            final ObjectSchema schema = namespace.requireObjectSchema(refKey.getSchema());
//
//                            result.put(ref, schema.expand(object, new Expander() {
//                                @Override
//                                public Instance ref(final Instance ref, final Set<Path> expand) {
//
//                                    final ExpandKey<RefKey> expandKey = ExpandKey.from(RefKey.from(ref), expand);
//                                    Instance result = expanded.get(expandKey);
//                                    if (result == null) {
//                                        final Map<String, Object> data = new HashMap<>(ref);
//                                        Instance.setHash(data, null);
//                                        result = new Instance(data);
//                                    }
//                                    return result;
//                                }
//
//                                @Override
//                                public PagedList<Instance> link(final String link, final Set<Path> expand) {
//
//                                    final ExpandKey<LinkKey> linkKey = ExpandKey.from(LinkKey.from(refKey, link), expand);
//                                    final CompletableFuture<PagedList<Instance>> future = links.get(linkKey);
//                                    if(future != null) {
//                                        final PagedList<Instance> result = future.getNow(null);
//                                        assert result != null;
//                                        return result;
//                                    } else {
//                                        return null;
//                                    }
//                                }
//                            }, ref.getExpand()));
//
//                        });
//
//                        return result;
//                    });
//
//                });
//            });
//        }
//    }
//
//    @Data
//    protected static class RefKey {
//
//        private final String schema;
//
//        private final String id;
//
//        public static RefKey from(final Instance item) {
//
//            return new RefKey(Instance.getSchema(item), Instance.getId(item));
//        }
//    }
//
//    @Data
//    protected static class LinkKey {
//
//        private final String schema;
//
//        private final String id;
//
//        private final String link;
//
//        public static LinkKey from(final Instance item, final String link) {
//
//            return new LinkKey(Instance.getSchema(item), Instance.getId(item), link);
//        }
//
//        public static LinkKey from(final RefKey ref, final String link) {
//
//            return new LinkKey(ref.getSchema(), ref.getId(), link);
//        }
//    }
//
//    @Data
//    protected static class ExpandKey<T> {
//
//        private final T key;
//
//        private final Set<Path> expand;
//
//        public static <T> ExpandKey<T> from(final T key, final Set<Path> expand) {
//
//            return new ExpandKey<>(key, expand);
//        }
//    }
//}

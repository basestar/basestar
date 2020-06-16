package io.basestar.storage;

/*-
 * #%L
 * basestar-storage
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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.schema.*;
import io.basestar.storage.aggregate.Aggregate;
import io.basestar.storage.exception.ObjectExistsException;
import io.basestar.storage.exception.VersionMismatchException;
import io.basestar.storage.query.Range;
import io.basestar.storage.util.Pager;
import io.basestar.util.PagedList;
import io.basestar.util.PagingToken;
import io.basestar.util.Path;
import io.basestar.util.Sort;
import lombok.Data;
import lombok.Setter;
import lombok.experimental.Accessors;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

// TODO optimize, this is currently used only as a mock so not important but should be a viable implementation

public class MemoryStorage extends PartitionedStorage {

    private State state = new State();

    private final Object lock = new Object();

    private MemoryStorage(final Builder builder) {

    }

    public static Builder builder() {

        return new Builder();
    }

    @Setter
    @Accessors(chain = true)
    public static class Builder {

        public MemoryStorage build() {

            return new MemoryStorage(this);
        }
    }

    @Override
    public CompletableFuture<Map<String, Object>> readObject(final ObjectSchema schema, final String id) {

        return CompletableFuture.supplyAsync(() -> {
            synchronized (lock) {
                return state.objects.get(new TypeId(schema.getName(), id));
            }
        });
    }

    @Override
    public CompletableFuture<Map<String, Object>> readObjectVersion(final ObjectSchema schema, final String id, final long version) {

        return CompletableFuture.supplyAsync(() -> {
            synchronized (lock) {
                return state.history.get(new TypeIdVersion(schema.getName(), id, version));
            }
        });
    }

    @Override
    public List<Pager.Source<Map<String, Object>>> aggregate(final ObjectSchema schema, final Expression query, final Map<String, Expression> group, final Map<String, Aggregate> aggregates) {

        // FIXME: only implemented for testing, this is equivalent to an all-objects-scan
        synchronized (lock) {

            final Multimap<Map<String, Object>, Map<String, Object>> groups = HashMultimap.create();

            state.objects.forEach((typeId, object) -> {
                if(typeId.getType().equals(schema.getName())) {
                    final Map<String, Object> g = group.entrySet().stream().collect(Collectors.toMap(
                            Map.Entry::getKey,
                            e -> e.getValue().evaluate(Context.init(object))
                    ));
                    groups.put(g, object);
                }
            });

            final List<Map<String, Object>> page = new ArrayList<>();
            groups.asMap().forEach((key, values) -> {
                final Map<String, Object> row = new HashMap<>();
                key.forEach(row::put);
                aggregates.forEach((name, agg) -> {
                    values.forEach(value -> {
                        final Object col = agg.evaluate(Context.init(), values.stream());
                        row.put(name, col);
                    });
                });
                page.add(row);
            });

            return ImmutableList.of(
                    (count, token) -> CompletableFuture.completedFuture(new PagedList<>(page, null))
            );
        }
    }

    @Override
    protected CompletableFuture<PagedList<Map<String, Object>>> queryIndex(final ObjectSchema schema, final Index index, final SatisfyResult satisfy, final Map<Path, Range<Object>> query, final List<Sort> sort, final int count, final PagingToken paging) {

        return CompletableFuture.supplyAsync(() -> {
            synchronized (lock) {

                final byte[] partBinary = binary(satisfy.getPartition());
                final IndexPartition partKey = new IndexPartition(schema.getName(), index.getName(), partBinary);

                final NavigableMap<IndexSort, Map<String, Object>> partition = state.index.get(partKey);

                final List<Map<String, Object>> results;
                if(partition == null) {
                    results = Collections.emptyList();
                } else {
                    if(!satisfy.getSort().isEmpty()) {
                        final byte[] sortLo = binary(satisfy.getSort());
                        final byte[] sortHi = binary(satisfy.getSort(), new byte[]{0});
                        results = Lists.newArrayList(partition.tailMap(new IndexSort(sortLo, null), true)
                                .headMap(new IndexSort(sortHi, null)).values());
                    } else {
                        results = Lists.newArrayList(partition.values());
                    }
                }

                return new PagedList<>(results, null);
            }
        });
    }

    @Override
    public ReadTransaction read(final Consistency consistency) {

        return new ReadTransaction() {

            private final List<CompletableFuture<BatchResponse>> futures = new ArrayList<>();

            @Override
            public ReadTransaction readObject(final ObjectSchema schema, final String id) {

                final CompletableFuture<Map<String, Object>> future = CompletableFuture.supplyAsync(() -> {
                    synchronized (lock) {
                        return state.objects.get(new TypeId(schema.getName(), id));
                    }
                });
                futures.add(future.thenApply(v -> BatchResponse.single(schema.getName(), v)));

                return this;
            }

            @Override
            public ReadTransaction readObjectVersion(final ObjectSchema schema, final String id, final long version) {

                final CompletableFuture<Map<String, Object>> future = CompletableFuture.supplyAsync(() -> {
                    synchronized (lock) {
                        return state.history.get(new TypeIdVersion(schema.getName(), id, version));
                    }
                });
                futures.add(future.thenApply(v -> BatchResponse.single(schema.getName(), v)));

                return this;
            }

            @Override
            public CompletableFuture<BatchResponse> read() {

                return BatchResponse.mergeFutures(futures.stream());
//
//                return CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[0]))
//                        .thenApply(ignored -> {
//                                final List<Map<String, Object>> results = new ArrayList<>();
//                                futures.forEach(future -> {
//                                    final Map<String, Object> result = future.getNow(null);
//                                    if(result != null) {
//                                        results.add(result);
//                                    }
//                                });
//                                return new BatchResponse.Basic(results);
//                        });
            }
        };
    }

    @Override
    public WriteTransaction write(final Consistency consistency) {

        return new WriteTransaction() {

            private final List<Function<State, BatchResponse>> items = new ArrayList<>();

            @Override
            public WriteTransaction createObject(final ObjectSchema schema, final String id, final Map<String, Object> after) {

                items.add(state -> {

                    final TypeId typeId = new TypeId(schema.getName(), id);
                    if(state.objects.containsKey(typeId)) {
                        throw new ObjectExistsException(schema.getName(), id);
                    } else {
                        state.objects.put(typeId, after);
                    }
                    final History history = schema.getHistory();
                    if(history.isEnabled() && history.getConsistency(Consistency.ATOMIC).isStronger(Consistency.ASYNC)) {
                        state.history.put(new TypeIdVersion(schema.getName(), id, 1L), after);
                    }
                    return BatchResponse.single(schema.getName(), after);
                });

                return createIndexes(schema, id, after);
            }

            private boolean checkExists(final Map<String, Object> current, final Long version) {

                if(current == null) {
                    return false;
                } else if(version != null) {
                    return version.equals(Instance.getVersion(current));
                } else {
                    return true;
                }
            }

            @Override
            public WriteTransaction updateObject(final ObjectSchema schema, final String id, final Map<String, Object> before, final Map<String, Object> after) {

                items.add(state -> {

                    final Long version = before == null ? null : Instance.getVersion(before);

                    final TypeId typeId = new TypeId(schema.getName(), id);
                    final Map<String, Object> current = state.objects.get(typeId);
                    if(checkExists(current, version)) {
                        state.objects.put(typeId, after);
                    } else {
                        throw new VersionMismatchException(schema.getName(), id, version);
                    }
                    final History history = schema.getHistory();
                    if(history.isEnabled() && history.getConsistency(Consistency.ATOMIC).isStronger(Consistency.ASYNC)) {
                        final Long afterVersion = Instance.getVersion(after);
                        assert afterVersion != null;
                        state.history.put(new TypeIdVersion(schema.getName(), id, afterVersion), after);
                    }
                    return BatchResponse.single(schema.getName(), after);
                });

                return updateIndexes(schema, id, before, after);
            }

            @Override
            public WriteTransaction deleteObject(final ObjectSchema schema, final String id, final Map<String, Object> before) {

                items.add(state -> {

                    final Long version = before == null ? null : Instance.getVersion(before);

                    final TypeId typeId = new TypeId(schema.getName(), id);
                    final Map<String, Object> current = state.objects.get(typeId);
                    if(checkExists(current, version)) {
                        state.objects.remove(typeId);
                    } else {
                        throw new VersionMismatchException(schema.getName(), id, version);
                    }
                    return BatchResponse.empty();
                });

                return deleteIndexes(schema, id, before);
            }

            @Override
            public WriteTransaction createIndex(final ObjectSchema schema, final Index index, final String id, final long version, final Index.Key key, final Map<String, Object> projection) {

                items.add(state -> {

                    final IndexPartition partKey = new IndexPartition(schema.getName(), index.getName(), binary(key.getPartition()));
                    final IndexSort sortKey = new IndexSort(binary(key.getSort()), index.isUnique() ? null : id);

                    final Map<IndexSort, Map<String, Object>> partition = state.index
                            .computeIfAbsent(partKey, k -> new TreeMap<>());

                    if(partition.containsKey(sortKey)) {
                        throw new IllegalStateException();
                    } else {
                        partition.put(sortKey, projection);
                    }

                    return BatchResponse.empty();
                });

                return this;
            }

            @Override
            public WriteTransaction updateIndex(final ObjectSchema schema, final Index index, final String id, final long version, final Index.Key key, final Map<String, Object> projection) {

                items.add(state -> {

                    final IndexPartition partKey = new IndexPartition(schema.getName(), index.getName(), binary(key.getPartition()));
                    final IndexSort sortKey = new IndexSort(binary(key.getSort()), index.isUnique() ? null : id);

                    final Map<IndexSort, Map<String, Object>> partition = state.index
                            .computeIfAbsent(partKey, k -> new TreeMap<>());

                    partition.put(sortKey, projection);

                    return BatchResponse.empty();
                });
                return this;
            }

            @Override
            public WriteTransaction deleteIndex(final ObjectSchema schema, final Index index, final String id, final long version, final Index.Key key) {

                items.add(state -> {

                    final IndexPartition partKey = new IndexPartition(schema.getName(), index.getName(), binary(key.getPartition()));
                    final IndexSort sortKey = new IndexSort(binary(key.getSort()), index.isUnique() ? null : id);

                    final Map<IndexSort, Map<String, Object>> partition = state.index
                            .computeIfAbsent(partKey, k -> new TreeMap<>());

                    partition.remove(sortKey);

                    return BatchResponse.empty();
                });
                return this;
            }

            @Override
            public Storage.WriteTransaction createHistory(final ObjectSchema schema, final String id, final long version, final Map<String, Object> after) {

                items.add(state -> {

                    final Long afterVersion = Instance.getVersion(after);
                    assert afterVersion != null;
                    state.history.put(new TypeIdVersion(schema.getName(), id, afterVersion), after);

                    return BatchResponse.empty();
                });

                return this;
            }

            @Override
            public CompletableFuture<BatchResponse> commit() {

                return CompletableFuture.supplyAsync(() -> {
                    final SortedMap<BatchResponse.Key, Map<String, Object>> changes = new TreeMap<>();
                    synchronized (lock) {
                        final State copy = state.copy();
                        items.forEach(item -> changes.putAll(item.apply(copy)));
                        state = copy;
                    }
                    return new BatchResponse.Basic(changes);
                });
            }
        };
    }

    @Override
    public EventStrategy eventStrategy(final ObjectSchema schema) {

        return EventStrategy.EMIT;
    }

    @Override
    public StorageTraits storageTraits(final ObjectSchema schema) {

        return MemoryStorageTraits.INSTANCE;
    }

    @Data
    private static class TypeId {

        private final String type;

        private final String id;
    }

    @Data
    private static class TypeIdVersion {

        private final String type;

        private final String id;

        private final long version;
    }

    @Data
    private static class IndexPartition {

        private final String type;

        private final String index;

        private final byte[] partition;
    }

    @Data
    private static class IndexSort implements Comparable<IndexSort> {

        private final byte[] range;

        private final String id;

        @Override
        public int compareTo(@Nonnull final IndexSort other) {

            // Sort must be compatible
            //assert(range.length == other.range.length);
            for(int i = 0; i != Math.max(range.length, other.range.length); ++i) {
                if(i >= range.length) {
                    return -1;
                } else if(i >= other.range.length) {
                    return 1;
                }
                final byte a = range[i];
                final byte b = other.range[i];
                final int compare = Byte.compare(a, b);
                if(compare != 0) {
                    return compare;
                }
            }
            return Comparator.<String>nullsFirst(Comparator.naturalOrder()).compare(id, other.id);
//            if(id == null && other.id == null) {
//                return 0;
//            } else if(id == null && other.id != null) {
//                return -1;
//            } else if(other.id == null) {
//                return 1;
//            } else {
//                return id.compareTo(other.id);
//            }
        }
    }

    private static class State {

        private final Map<TypeId, Map<String, Object>> objects = new HashMap<>();

        private final Map<TypeIdVersion, Map<String, Object>> history = new HashMap<>();

        private final Map<IndexPartition, NavigableMap<IndexSort, Map<String, Object>>> index = new HashMap<>();

        public State copy() {

            final State result = new State();
            result.objects.putAll(objects);
            result.history.putAll(history);
            result.index.putAll(index);
            return result;
        }
    }
}

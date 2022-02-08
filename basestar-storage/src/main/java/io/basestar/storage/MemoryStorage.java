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

import com.google.common.collect.Lists;
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.schema.*;
import io.basestar.storage.annotation.ConfigurableStorage;
import io.basestar.storage.exception.ObjectExistsException;
import io.basestar.storage.exception.VersionMismatchException;
import io.basestar.storage.query.Range;
import io.basestar.util.*;
import lombok.Data;
import lombok.Setter;
import lombok.experimental.Accessors;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

// TODO optimize, this is currently used only as a mock so not important but should be a viable implementation

@ConfigurableStorage(builderClass = MemoryStorage.Builder.class)
public class MemoryStorage implements DefaultIndexStorage {

    private State state = new State();

    private final Object lock = new Object();

    private MemoryStorage(final Builder builder) {

    }

    public static Builder builder() {

        return new Builder();
    }

    @Setter
    @Accessors(chain = true)
    public static class Builder implements Storage.Builder {

        public MemoryStorage build() {

            return new MemoryStorage(this);
        }
    }

    @Override
    public Pager<Map<String, Object>> queryIndex(final ObjectSchema schema, final Index index, final SatisfyResult satisfy, final Map<Name, Range<Object>> query, final List<Sort> sort, final Set<Name> expand) {

        synchronized (lock) {

            final byte[] partBinary = BinaryKey.from(satisfy.getPartition()).getBytes();
            final IndexPartition partKey = new IndexPartition(schema.getQualifiedName(), index.getName(), partBinary);

            final NavigableMap<IndexSort, Map<String, Object>> partition = state.index.get(partKey);

            final List<Map<String, Object>> results;
            if(partition == null) {
                results = Collections.emptyList();
            } else {
                if(!satisfy.getSort().isEmpty()) {
                    final byte[] sortLo = BinaryKey.from(satisfy.getSort()).lo().getBytes();
                    final byte[] sortHi = BinaryKey.from(satisfy.getSort()).hi().getBytes();
                    results = Lists.newArrayList(partition.tailMap(new IndexSort(sortLo, null), true)
                            .headMap(new IndexSort(sortHi, null)).values());
                } else {
                    results = Lists.newArrayList(partition.values());
                }
            }

            return Pager.simple(results);
        }
    }

    @Override
    public Pager<Map<String, Object>> queryHistory(final Consistency consistency, final ReferableSchema schema, final String id, final Expression query, final List<Sort> sort, final Set<Name> expand) {

        synchronized (lock) {

            final List<Map<String, Object>> results = state.history.entrySet().stream().filter(entry -> {
                        final SchemaIdVersion key = entry.getKey();
                        final Map<String, Object> value = entry.getValue();
                        if (key.getSchema().equals(schema.getQualifiedName()) && key.getId().equals(id)) {
                            return query.evaluatePredicate(Context.init(value));
                        } else {
                            return false;
                        }
                    }).map(Map.Entry::getValue)
                    .sorted(Instance.comparator(sort))
                    .collect(Collectors.toList());

            return Pager.simple(results);
        }
    }

    @Override
    public Pager<Map<String, Object>> queryHistoryRange(final Consistency consistency, final ObjectSchema schema, final String id, final Map<Name, Range<Object>> query, final Sort.Order order, final Set<Name> expand) {

        synchronized (lock) {

            final List<Map<String, Object>> results = state.history.entrySet().stream().filter(entry -> {
                        final SchemaIdVersion key = entry.getKey();
                        final Map<String, Object> value = entry.getValue();
                        if (key.getSchema().equals(schema.getQualifiedName()) && key.getId().equals(id)) {
                            return query.entrySet().stream().allMatch(q -> q.getValue().test(Instance.get(value, q.getKey())));
                        } else {
                            return false;
                        }
                    }).map(Map.Entry::getValue)
                    .sorted(Instance.comparator(new Sort(Name.of(ReferableSchema.VERSION), order)))
                    .collect(Collectors.toList());

            return Pager.simple(results);
        }
    }

    @Override
    public Scan scan(final ReferableSchema schema, final Expression query, final int segments) {

        return new Scan() {
            @Override
            public int getSegments() {

                return 1;
            }

            @Override
            public Segment segment(final int segment) {

                assert segment == 0;
                final List<Map<String, Object>> objects = new ArrayList<>();
                synchronized (lock) {
                    state.objects.forEach((k, v) -> {
                        if(k.getSchema().equals(schema.getQualifiedName())) {
                            objects.add(v);
                        }
                    });
                }
                return Segment.fromIterator(objects.iterator());
            }
        };
    }

    @Override
    public ReadTransaction read(final Consistency consistency) {

        return new ReadTransaction() {

            private final BatchCapture capture = new BatchCapture();

            @Override
            public ReadTransaction getObject(final ObjectSchema schema, final String id, final Set<Name> expand) {

                capture.captureLatest(schema, id, expand);
                return this;
            }

            @Override
            public ReadTransaction getObjectVersion(final ObjectSchema schema, final String id, final long version, final Set<Name> expand) {

                capture.captureVersion(schema, id, version, expand);
                return this;
            }

            @Override
            public CompletableFuture<BatchResponse> read() {

                return CompletableFuture.supplyAsync(() -> {
                    final Map<BatchResponse.RefKey, Map<String, Object>> results = new HashMap<>();

                    capture.getRefs().forEach((schema, refs) -> {
                        refs.forEach((key, args) -> {
                            synchronized (lock) {
                                if (key.hasVersion()) {
                                    results.put(key, state.history.get(new SchemaIdVersion(schema.getQualifiedName(), key.getId(), key.getVersion())));
                                } else {
                                    results.put(key, state.objects.get(new SchemaId(schema.getQualifiedName(), key.getId())));
                                }
                            }
                        });
                    });

                    return BatchResponse.fromRefs(results);
                });
            }
        };
    }

    @Override
    public WriteTransaction write(final Consistency consistency, final Versioning versioning) {

        return new WriteTransaction() {

            private final List<Function<State, BatchResponse>> items = new ArrayList<>();

            @Override
            public StorageTraits storageTraits(final ReferableSchema schema) {

                return MemoryStorage.this.storageTraits(schema);
            }

            @Override
            public void writeObjectLayer(final ReferableSchema schema, final Map<String, Object> after) {

                throw new UnsupportedOperationException();
            }

            @Override
            public void createObjectLayer(final ReferableSchema schema, final String id, final Map<String, Object> after) {

                items.add(state -> {

                    final SchemaId typeId = new SchemaId(schema.getQualifiedName(), id);
                    if(state.objects.containsKey(typeId)) {
                        throw new ObjectExistsException(schema.getQualifiedName(), id);
                    } else {
                        state.objects.put(typeId, after);
                    }
//                    final History history = schema.getHistory();
//                    if(history.isEnabled() && history.getConsistency(Consistency.ATOMIC).isStronger(Consistency.ASYNC)) {
//                        state.history.put(new SchemaIdVersion(schema.getQualifiedName(), id, 1L), after);
//                    }
                    return BatchResponse.fromRef(schema.getQualifiedName(), after);
                });
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
            public void updateObjectLayer(final ReferableSchema schema, final String id, final Map<String, Object> before, final Map<String, Object> after) {

                items.add(state -> {

                    final Long version = Nullsafe.map(before, Instance::getVersion);
                    final SchemaId typeId = new SchemaId(schema.getQualifiedName(), id);
                    final Map<String, Object> current = state.objects.get(typeId);
                    if(checkExists(current, version)) {
                        state.objects.put(typeId, after);
                    } else {
                        throw new VersionMismatchException(schema.getQualifiedName(), id, version);
                    }
//                    final History history = schema.getHistory();
//                    if(history.isEnabled() && history.getConsistency(Consistency.ATOMIC).isStronger(Consistency.ASYNC)) {
//                        final Long afterVersion = Instance.getVersion(after);
//                        assert afterVersion != null;
//                        state.history.put(new SchemaIdVersion(schema.getQualifiedName(), id, afterVersion), after);
//                    }
                    return BatchResponse.fromRef(schema.getQualifiedName(), after);
                });
            }

            @Override
            public void deleteObjectLayer(final ReferableSchema schema, final String id, final Map<String, Object> before) {

                items.add(state -> {

                    final Long version = Nullsafe.map(before, Instance::getVersion);
                    final SchemaId typeId = new SchemaId(schema.getQualifiedName(), id);
                    final Map<String, Object> current = state.objects.get(typeId);
                    if(checkExists(current, version)) {
                        state.objects.remove(typeId);
                    } else {
                        throw new VersionMismatchException(schema.getQualifiedName(), id, version);
                    }
                    return BatchResponse.empty();
                });
            }

            @Override
            public void writeHistoryLayer(final ReferableSchema schema, final String id, final Map<String, Object> after) {

                items.add(state -> {

                    state.history.put(new SchemaIdVersion(schema.getQualifiedName(), id, Instance.getVersion(after)), after);

                    return BatchResponse.empty();
                });
            }

            @Override
            public void createIndex(final ReferableSchema schema, final Index index, final String id, final long version, final Index.Key key, final Map<String, Object> projection) {

                withPartitionSort(schema, index, id, key, (partition, sortKey) -> {

                    if (partition.containsKey(sortKey)) {
                        throw new IllegalStateException();
                    } else {
                        partition.put(sortKey, projection);
                    }
                });
            }

            @Override
            public void updateIndex(final ReferableSchema schema, final Index index, final String id, final long version, final Index.Key key, final Map<String, Object> projection) {

                withPartitionSort(schema, index, id, key, (partition, sortKey) -> partition.put(sortKey, projection));
            }

            @Override
            public void deleteIndex(final ReferableSchema schema, final Index index, final String id, final long version, final Index.Key key) {

                withPartitionSort(schema, index, id, key, Map::remove);
            }

            private void withPartitionSort(final ReferableSchema schema, final Index index, final String id, final Index.Key key, final BiConsumer<Map<IndexSort, Map<String, Object>>, IndexSort> consumer) {

                items.add(state -> {

                    final Index.Key.Binary binaryKey = key.binary();
                    final IndexPartition partKey = new IndexPartition(schema.getQualifiedName(), index.getName(), binaryKey.getPartition().getBytes());
                    final IndexSort sortKey = new IndexSort(binaryKey.getSort().getBytes(), index.isUnique() ? null : id);

                    final Map<IndexSort, Map<String, Object>> partition = state.index
                            .computeIfAbsent(partKey, k -> new TreeMap<>());

                    consumer.accept(partition, sortKey);

                    return BatchResponse.empty();
                });
            }

            @Override
            public CompletableFuture<BatchResponse> write() {

                return CompletableFuture.supplyAsync(() -> {
                    final SortedMap<BatchResponse.RefKey, Map<String, Object>> changes = new TreeMap<>();
                    synchronized (lock) {
                        final State copy = state.copy();
                        items.forEach(item -> changes.putAll(item.apply(copy).getRefs()));
                        state = copy;
                    }
                    return BatchResponse.fromRefs(changes);
                });
            }
        };
    }

    @Override
    public EventStrategy eventStrategy(final ReferableSchema schema) {

        return EventStrategy.EMIT;
    }

    @Override
    public StorageTraits storageTraits(final Schema schema) {

        return TRAITS;
    }

    @Override
    public CompletableFuture<Long> increment(final SequenceSchema schema) {

        return CompletableFuture.supplyAsync(() -> {

            final Long result;
            synchronized (lock) {
                final State copy = state.copy();
                result = copy.sequences.compute(schema.getQualifiedName(), (k, v) -> {
                    if (v == null) {
                        return schema.getEffectiveStart();
                    } else {
                        return v + schema.getEffectiveIncrement();
                    }
                });
                state = copy;
            }
            return result;
        });
    }

    @Data
    private static class SchemaId {

        private final Name schema;

        private final String id;
    }

    @Data
    private static class SchemaIdVersion {

        private final Name schema;

        private final String id;

        private final long version;
    }

    @Data
    private static class IndexPartition {

        private final Name schema;

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
        }
    }

    private static class State {

        private final Map<SchemaId, Map<String, Object>> objects = new HashMap<>();

        private final Map<SchemaIdVersion, Map<String, Object>> history = new HashMap<>();

        private final Map<IndexPartition, NavigableMap<IndexSort, Map<String, Object>>> index = new HashMap<>();

        private final Map<Name, Long> sequences = new HashMap<>();

        public State copy() {

            final State result = new State();
            result.objects.putAll(objects);
            result.history.putAll(history);
            result.index.putAll(index);
            result.sequences.putAll(sequences);
            return result;
        }
    }

    private static final StorageTraits TRAITS = new StorageTraits() {

        @Override
        public Consistency getHistoryConsistency() {

            return Consistency.ATOMIC;
        }

        @Override
        public Consistency getSingleValueIndexConsistency() {

            return Consistency.ATOMIC;
        }

        @Override
        public Consistency getMultiValueIndexConsistency() {

            return Consistency.ATOMIC;
        }

        @Override
        public boolean supportsPolymorphism() {

            return true;
        }

        @Override
        public boolean supportsMultiObject() {

            return true;
        }

        @Override
        public Concurrency getObjectConcurrency() {

            return Concurrency.OPTIMISTIC;
        }

        @Override
        public boolean supportsSequence() {

            return true;
        }
    };
}

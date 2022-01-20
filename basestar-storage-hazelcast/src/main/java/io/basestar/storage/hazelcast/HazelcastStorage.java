package io.basestar.storage.hazelcast;

/*-
 * #%L
 * basestar-storage-hazelcast
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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.query.Predicate;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.transaction.TransactionalMap;
import io.basestar.expression.Expression;
import io.basestar.schema.*;
import io.basestar.storage.*;
import io.basestar.storage.exception.ObjectExistsException;
import io.basestar.storage.exception.VersionMismatchException;
import io.basestar.storage.hazelcast.serde.CustomPortable;
import io.basestar.storage.hazelcast.serde.PortableSchemaFactory;
import io.basestar.util.Name;
import io.basestar.util.Nullsafe;
import io.basestar.util.Pager;
import io.basestar.util.Sort;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

@Slf4j
public class HazelcastStorage implements DefaultLayerStorage {

    @Nonnull
    private final HazelcastInstance instance;

    @Nonnull
    private final HazelcastStrategy strategy;

    @Nonnull
    private final PortableSchemaFactory schemaFactory;

    private final LoadingCache<ReferableSchema, IMap<BatchResponse.RefKey, CustomPortable>> object;

    private final LoadingCache<ReferableSchema, IMap<BatchResponse.RefKey, CustomPortable>> history;

    private HazelcastStorage(final Builder builder) {

        this.instance = Nullsafe.require(builder.instance);
        this.strategy = Nullsafe.require(builder.strategy);
        this.schemaFactory = Nullsafe.require(builder.schemaFactory);
        this.object = CacheBuilder.newBuilder()
                .build(new CacheLoader<ReferableSchema, IMap<BatchResponse.RefKey, CustomPortable>>() {
                    @Override
                    public IMap<BatchResponse.RefKey, CustomPortable> load(final ReferableSchema s) {
                        return instance.getMap(strategy.objectMapName(s));
                    }
                });
        this.history = CacheBuilder.newBuilder()
                .build(new CacheLoader<ReferableSchema, IMap<BatchResponse.RefKey, CustomPortable>>() {
                    @Override
                    public IMap<BatchResponse.RefKey, CustomPortable> load(final ReferableSchema s) {
                        return instance.getMap(strategy.historyMapName(s));
                    }
                });
    }

    public static Builder builder() {

        return new Builder();
    }

    @Setter
    @Accessors(chain = true)
    public static class Builder {

        @Nullable
        private HazelcastInstance instance;

        @Nullable
        private HazelcastStrategy strategy;

        @Nullable
        private PortableSchemaFactory schemaFactory;

        public HazelcastStorage build() {

            return new HazelcastStorage(this);
        }
    }

    private Map<String, Object> fromRecord(final CustomPortable record) {

        return record == null ? null : record.getData();
    }

    private CustomPortable toRecord(final InstanceSchema schema, final Map<String, Object> data) {

        final CustomPortable record = schemaFactory.create(schema);
        record.setData(data);
        return record;
    }

//    @Override
//    public CompletableFuture<Map<String, Object>> readObject(final ObjectSchema schema, final String id, final Set<Name> expand) {
//
//        try {
//            final IMap<BatchResponse.RefKey, CustomPortable> map = object.get(schema);
//
//            return map.getAsync(BatchResponse.RefKey.latest(schema.getQualifiedName(), id))
//                    .thenApply(this::fromRecord)
//                    .toCompletableFuture();
//        } catch (final ExecutionException e) {
//            throw new IllegalStateException(e);
//        }
//    }
//    @Override
//    public CompletableFuture<Map<String, Object>> readObjectVersion(final ObjectSchema schema, final String id, final long version, final Set<Name> expand) {
//
//        try {
//            final IMap<BatchResponse.RefKey, CustomPortable> map = history.get(schema);
//
//            return map.getAsync(BatchResponse.RefKey.version(schema.getQualifiedName(), id, version))
//                    .thenApply(this::fromRecord)
//                    .toCompletableFuture();
//        } catch (final ExecutionException e) {
//            throw new IllegalStateException(e);
//        }
//    }

    @Override
    public Pager<Map<String, Object>> queryObject(final Consistency consistency, final ObjectSchema schema, final Expression query, final List<Sort> sort, final Set<Name> expand) {

        return Pager.simple(CompletableFuture.supplyAsync(() -> {
            try {
                final Predicate<BatchResponse.RefKey, CustomPortable> predicate = query.visit(new HazelcastExpressionVisitor<>());
                final IMap<BatchResponse.RefKey, CustomPortable> map = object.get(schema);

                final List<Map<String, Object>> results = new ArrayList<>();
                for (final Map.Entry<BatchResponse.RefKey, CustomPortable> entry : map.entrySet(predicate)) {
                    results.add(fromRecord(entry.getValue()));
                }
                results.sort(Instance.comparator(sort));

                // TODO: implement keyset paging

                return results;

            } catch (final ExecutionException e) {
                throw new IllegalStateException(e);
            }
        }));
    }

    @Override
    public ReadTransaction read(final Consistency consistency) {

        return new ReadTransaction() {

            final BatchCapture capture = new BatchCapture();

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
                    final Map<BatchResponse.RefKey, Map<String, Object>> refs = new HashMap<>();
                    capture.forEachRef((schema, ref, expand) -> {
                        final String target;
                        if (ref.hasVersion()) {
                            target = strategy.historyMapName(schema);
                        } else {
                            target = strategy.objectMapName(schema);
                        }
                        final IMap<BatchResponse.RefKey, CustomPortable> map = instance.getMap(target);
                        refs.put(ref, fromRecord(map.get(ref)));
                    });
                    return BatchResponse.fromRefs(refs);
                });
            }
        };
    }

    private interface WriteAction extends Serializable  {

        CustomPortable apply(BatchResponse.RefKey key, CustomPortable value);
    }

    protected class WriteTransaction implements DefaultLayerStorage.WriteTransaction {

        private final Map<String, Map<BatchResponse.RefKey, WriteAction>> requests = new IdentityHashMap<>();

        @Override
        public StorageTraits storageTraits(final ReferableSchema schema) {

            return HazelcastStorage.this.storageTraits(schema);
        }

        @Override
        public void createObjectLayer(final ReferableSchema schema, final String id, final Map<String, Object> after) {

            final String target = strategy.objectMapName(schema);
            final Name schemaName = schema.getQualifiedName();
            requests.computeIfAbsent(target, ignored -> new HashMap<>())
                    .put(BatchResponse.RefKey.latest(schemaName, id), (key, value) -> {
                        if(value == null) {
                            return toRecord(schema, after);
                        } else {
                            throw new ObjectExistsException(schemaName, id);
                        }
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

            final String target = strategy.objectMapName(schema);
            final Name schemaName = schema.getQualifiedName();
            final Long version = before == null ? null : Instance.getVersion(before);
            requests.computeIfAbsent(target, ignored -> new HashMap<>())
                    .put(BatchResponse.RefKey.latest(schemaName, id), (key, value) -> {
                        final Map<String, Object> current = fromRecord(value);
                        if(checkExists(current, version)) {
                            return toRecord(schema, after);
                        } else {
                            throw new VersionMismatchException(schemaName, id, version);
                        }
                    });
        }

        @Override
        public void deleteObjectLayer(final ReferableSchema schema, final String id, final Map<String, Object> before) {

            final String target = strategy.objectMapName(schema);
            final Name schemaName = schema.getQualifiedName();
            final Long version = before == null ? null : Instance.getVersion(before);
            requests.computeIfAbsent(target, ignored -> new HashMap<>())
                    .put(BatchResponse.RefKey.latest(schemaName, id), (key, value) -> {
                        final Map<String, Object> current = fromRecord(value);
                        if(checkExists(current, version)) {
                            return null;
                        } else {
                            throw new VersionMismatchException(schemaName, id, version);
                        }
                    });
        }

        @Override
        public void writeHistoryLayer(final ReferableSchema schema, final String id, final Map<String, Object> after) {

            final long version = Instance.getVersion(after);
            final String target = strategy.historyMapName(schema);
            final Name schemaName = schema.getQualifiedName();
            requests.computeIfAbsent(target, ignored -> new HashMap<>())
                    .put(BatchResponse.RefKey.version(schemaName, id, version), (key, value) -> toRecord(schema, after));
        }

//        private WriteTransaction createHistory(final ObjectSchema schema, final String id, final Map<String, Object> after) {
//
//            final History history = schema.getHistory();
//            if(history.isEnabled() && history.getConsistency(Consistency.ATOMIC).isStronger(Consistency.ASYNC)) {
//                final Long afterVersion = Instance.getVersion(after);
//                assert afterVersion != null;
//                return createHistory(schema, id, afterVersion, after);
//            } else {
//                return this;
//            }
//        }

        @Override
        public CompletableFuture<BatchResponse> write() {

            return CompletableFuture.supplyAsync(() -> {

                final TransactionOptions options = new TransactionOptions().setTransactionType(TransactionOptions.TransactionType.TWO_PHASE);
                final TransactionContext context = instance.newTransactionContext(options);

                context.beginTransaction();

                final Map<BatchResponse.RefKey, Map<String, Object>> results = new HashMap<>();

                try {
                    requests.forEach((target, actions) -> {
                        final TransactionalMap<BatchResponse.RefKey, CustomPortable> map = context.getMap(target);

                        actions.forEach((key, action) -> {
                            final CustomPortable oldValue = map.getForUpdate(key);
                            final CustomPortable newValue = action.apply(key, oldValue);
                            if(newValue != null) {
                                map.put(key, newValue);
                            } else if(oldValue != null) {
                                map.delete(key);
                            }
                        });
                    });
                    context.commitTransaction();
                } catch (final Throwable e) {
                    log.error("Rolling back", e);
                    context.rollbackTransaction();
                    throw e;
                }

                return new BatchResponse(results);
            });
        }

        @Override
        public WriteTransaction write(final LinkableSchema schema, final Map<String, Object> after) {

            throw new UnsupportedOperationException();
        }
    }

    @Override
    public WriteTransaction write(final Consistency consistency, final Versioning versioning) {

        return new WriteTransaction();
    }

    @Override
    public EventStrategy eventStrategy(final ReferableSchema schema) {

        return EventStrategy.EMIT;
    }

    @Override
    public StorageTraits storageTraits(final Schema schema) {

        return HazelcastStorageTraits.INSTANCE;
    }

    @Override
    public CompletableFuture<Long> increment(final SequenceSchema schema) {

        // FIXME: could implement trivially
        throw new UnsupportedOperationException();
    }
}

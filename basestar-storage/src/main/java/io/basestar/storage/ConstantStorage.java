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

import com.google.common.collect.ImmutableMap;
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.schema.*;
import io.basestar.util.*;

import java.util.*;
import java.util.concurrent.CompletableFuture;

public class ConstantStorage implements DefaultLayerStorage {

    private final Map<Name, Map<String, Map<String, Object>>> data;

    @lombok.Builder(builderClassName = "Builder")
    ConstantStorage(final Collection<? extends Map<String, Object>> items) {

        final Map<Name, Map<String, Map<String, Object>>> data = new HashMap<>();
        items.forEach(item -> {
            final Name schema = Instance.getSchema(item);
            final String id = Instance.getId(item);
            final Map<String, Map<String, Object>> target = data.computeIfAbsent(schema, ignored -> new HashMap<>());
            target.put(id, item);
        });
        this.data = Collections.unmodifiableMap(data);
    }

    private Map<String, Object> get(final Name schema, final String id) {

        return Nullsafe.orDefault(data.get(schema)).get(id);
    }

    private Map<String, Object> get(final Map<String, Object> instance) {

        return get(Instance.getSchema(instance), Instance.getId(instance));
    }

    @Override
    public Pager<Map<String, Object>> queryObject(final Consistency consistency, final ObjectSchema schema, final Expression query, final List<Sort> sort, final Set<Name> expand) {

        final List<Map<String, Object>> all = new ArrayList<>();
        Nullsafe.orDefault(data.get(schema.getQualifiedName())).forEach((id, item) -> {
            if (query.evaluatePredicate(Context.init(item))) {
                all.add(item);
            }
        });

        // Must be sorted
        final Comparator<Map<String, Object>> comparator = Instance.comparator(sort);
        all.sort(comparator);

        return Pager.simple(all);
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
                final Collection<Map<String, Object>> objects = data.getOrDefault(schema.getQualifiedName(), ImmutableMap.of()).values();
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

                final Map<BatchResponse.RefKey, Map<String, Object>> refs = new HashMap<>();
                capture.forEachRef((schema, key, args) -> {
                    final Map<String, Object> object = ConstantStorage.this.get(key.getSchema(), key.getId());
                    refs.put(key, key.matchOrNull(object));
                });
                return CompletableFuture.completedFuture(BatchResponse.fromRefs(refs));
            }
        };
    }

    @Override
    public WriteTransaction write(final Consistency consistency, final Versioning versioning) {

        throw new UnsupportedOperationException();
    }

    @Override
    public EventStrategy eventStrategy(final ReferableSchema schema) {

        return EventStrategy.EMIT;
    }

    @Override
    public StorageTraits storageTraits(final ReferableSchema schema) {

        return TRAITS;
    }

    @Override
    public Pager<Map<String, Object>> queryHistory(final Consistency consistency, final ReferableSchema schema, final String id, final Expression query, final List<Sort> sort, final Set<Name> expand) {

        return (stats, token, count) -> get(consistency, schema, id, expand).thenApply(result -> {
            if (result == null) {
                return Page.empty();
            } else {
                return Page.single(result);
            }
        });
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
    };
}

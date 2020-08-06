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

import com.google.common.collect.ImmutableList;
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.schema.Concurrency;
import io.basestar.schema.Consistency;
import io.basestar.schema.Instance;
import io.basestar.schema.ObjectSchema;
import io.basestar.util.*;

import java.util.*;
import java.util.concurrent.CompletableFuture;

public class ConstantStorage implements Storage.WithoutAggregate, Storage.WithoutWrite, Storage.WithoutHistory, Storage.WithoutExpand {

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

        return Nullsafe.option(data.get(schema)).get(id);
    }

    private Map<String, Object> get(final Map<String, Object> instance) {

        return get(Instance.getSchema(instance), Instance.getId(instance));
    }

    @Override
    public CompletableFuture<Map<String, Object>> readObject(final ObjectSchema schema, final String id, final Set<Name> expand) {

        final Map<String, Object> object = get(schema.getQualifiedName(), id);
        return CompletableFuture.completedFuture(object);
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public List<Pager.Source<Map<String, Object>>> query(final ObjectSchema schema, final Expression query, final List<Sort> sort, final Set<Name> expand) {

        // Add a source that will emit matching results

        return ImmutableList.of((count, token, stats) -> {
            // Don't do any paging, just return all matches, this is compliant with Pager interface
            final List<Map<String, Object>> page = new ArrayList<>();
            Nullsafe.option(data.get(schema.getQualifiedName())).forEach((id, item) -> {
                if(query.evaluatePredicate(Context.init(item))) {
                    page.add(item);
                }
            });

            // Must be sorted
            final Comparator<Map<String, Object>> comparator = Instance.comparator(sort);
            page.sort(comparator);

            return CompletableFuture.completedFuture(new Page<>(page, null));
        });
    }

    @Override
    public ReadTransaction read(final Consistency consistency) {

        return new ReadTransaction.Basic(this);
    }

    @Override
    public EventStrategy eventStrategy(final ObjectSchema schema) {

        return EventStrategy.EMIT;
    }

    @Override
    public StorageTraits storageTraits(final ObjectSchema schema) {

        return TRAITS;
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

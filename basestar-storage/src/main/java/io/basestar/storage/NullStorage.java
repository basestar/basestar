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

import io.basestar.expression.Expression;
import io.basestar.expression.aggregate.Aggregate;
import io.basestar.schema.Concurrency;
import io.basestar.schema.Consistency;
import io.basestar.schema.ObjectSchema;
import io.basestar.util.Name;
import io.basestar.util.Page;
import io.basestar.util.Pager;
import io.basestar.util.Sort;
import lombok.RequiredArgsConstructor;

import java.util.*;
import java.util.concurrent.CompletableFuture;

@RequiredArgsConstructor
public class NullStorage implements Storage.WithoutWriteIndex, Storage.WithoutWriteHistory, Storage.WithoutExpand, Storage.WithoutRepair {

    private final EventStrategy eventStrategy;

    @Override
    public CompletableFuture<Map<String, Object>> readObject(final ObjectSchema schema, final String id, final Set<Name> expand) {

        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Map<String, Object>> readObjectVersion(final ObjectSchema schema, final String id, final long version, final Set<Name> expand) {

        return CompletableFuture.completedFuture(null);
    }

    @Override
    public List<Pager.Source<Map<String, Object>>> queryObject(final ObjectSchema schema, final Expression query, final List<Sort> sort, final Set<Name> expand) {

        return Collections.singletonList((count, pagingToken, stats) -> CompletableFuture.completedFuture(Page.empty()));
    }

    @Override
    public List<Pager.Source<Map<String, Object>>> aggregate(final ObjectSchema schema, final Expression query, final Map<String, Expression> group, final Map<String, Aggregate> aggregates) {

        return Collections.singletonList((count, pagingToken, stats) -> CompletableFuture.completedFuture(Page.empty()));
    }

    @Override
    public ReadTransaction read(final Consistency consistency) {

        return new ReadTransaction.Basic(this);
    }

    @Override
    public WriteTransaction write(final Consistency consistency, final Versioning versioning) {

        return new WriteTransaction() {

            private final Map<BatchResponse.Key, Map<String, Object>> data = new HashMap<>();

            @Override
            public WriteTransaction createObject(final ObjectSchema schema, final String id, final Map<String, Object> after) {

                data.put(BatchResponse.Key.from(schema.getQualifiedName(), after), after);
                return this;
            }

            @Override
            public WriteTransaction updateObject(final ObjectSchema schema, final String id, final Map<String, Object> before, final Map<String, Object> after) {

                data.put(BatchResponse.Key.from(schema.getQualifiedName(), after), after);
                return this;
            }

            @Override
            public WriteTransaction deleteObject(final ObjectSchema schema, final String id, final Map<String, Object> before) {

                return this;
            }

            @Override
            public CompletableFuture<BatchResponse> write() {

                return CompletableFuture.completedFuture(new BatchResponse.Basic(data));
            }
        };
    }

    @Override
    public EventStrategy eventStrategy(final ObjectSchema schema) {

        return eventStrategy;
    }

    @Override
    public StorageTraits storageTraits(final ObjectSchema schema) {

        return TRAITS;
    }

    public static final StorageTraits TRAITS = new StorageTraits() {

        @Override
        public Consistency getHistoryConsistency() {

            return Consistency.NONE;
        }

        @Override
        public Consistency getSingleValueIndexConsistency() {

            return Consistency.NONE;
        }

        @Override
        public Consistency getMultiValueIndexConsistency() {

            return Consistency.NONE;
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

            return Concurrency.NONE;
        }
    };
}

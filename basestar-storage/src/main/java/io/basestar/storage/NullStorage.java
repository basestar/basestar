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
import io.basestar.schema.*;
import io.basestar.util.Name;
import io.basestar.util.Pager;
import io.basestar.util.Sort;
import lombok.RequiredArgsConstructor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

@RequiredArgsConstructor
public class NullStorage implements DefaultLayerStorage {

    private final EventStrategy eventStrategy;

    @Override
    public Pager<Map<String, Object>> queryObject(final Consistency consistency, final ObjectSchema schema, final Expression query, final List<Sort> sort, final Set<Name> expand) {

        return Pager.empty();
    }

    @Override
    public Pager<Map<String, Object>> queryHistory(final Consistency consistency, final ReferableSchema schema, final String id, final Expression query, final List<Sort> sort, final Set<Name> expand) {

        return Pager.empty();
    }

    @Override
    public ReadTransaction read(final Consistency consistency) {

        return new ReadTransaction() {

            @Override
            public ReadTransaction getObject(final ObjectSchema schema, final String id, final Set<Name> expand) {

                return this;
            }

            @Override
            public ReadTransaction getObjectVersion(final ObjectSchema schema, final String id, final long version, final Set<Name> expand) {

                return this;
            }

            @Override
            public CompletableFuture<BatchResponse> read() {

                return CompletableFuture.completedFuture(BatchResponse.empty());
            }
        };
    }

    @Override
    public WriteTransaction write(final Consistency consistency, final Versioning versioning) {

        return new WriteTransaction() {

            private final Map<BatchResponse.RefKey, Map<String, Object>> data = new HashMap<>();

            @Override
            public StorageTraits storageTraits(final ReferableSchema schema) {

                return NullStorage.this.storageTraits(schema);
            }

            @Override
            public void writeObjectLayer(final ReferableSchema schema, final Map<String, Object> after) {

            }

            @Override
            public void createObjectLayer(final ReferableSchema schema, final String id, final Map<String, Object> after) {

                data.put(BatchResponse.RefKey.from(schema.getQualifiedName(), after), after);
            }

            @Override
            public void updateObjectLayer(final ReferableSchema schema, final String id, final Map<String, Object> before, final Map<String, Object> after) {

                data.put(BatchResponse.RefKey.from(schema.getQualifiedName(), after), after);
            }

            @Override
            public void deleteObjectLayer(final ReferableSchema schema, final String id, final Map<String, Object> before) {

            }

            @Override
            public void writeHistoryLayer(final ReferableSchema schema, final String id, final Map<String, Object> after) {

            }

            @Override
            public CompletableFuture<BatchResponse> write() {

                return CompletableFuture.completedFuture(BatchResponse.fromRefs(data));
            }
        };
    }

    @Override
    public EventStrategy eventStrategy(final ReferableSchema schema) {

        return eventStrategy;
    }

    @Override
    public StorageTraits storageTraits(final Schema schema) {

        return TRAITS;
    }

    @Override
    public CompletableFuture<Long> increment(final SequenceSchema schema) {

        throw new UnsupportedOperationException();
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

        @Override
        public boolean supportsSequence() {

            return false;
        }
    };
}

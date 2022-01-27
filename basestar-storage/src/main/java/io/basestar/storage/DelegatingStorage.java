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

import io.basestar.event.Event;
import io.basestar.expression.Expression;
import io.basestar.schema.*;
import io.basestar.util.CompletableFutures;
import io.basestar.util.Name;
import io.basestar.util.Pager;
import io.basestar.util.Sort;

import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public interface DelegatingStorage extends Storage {

    Storage storage(Schema schema);

    @Override
    default void validate(final ObjectSchema schema) {

        storage(schema).validate(schema);
    }

    @Override
    default EventStrategy eventStrategy(final ReferableSchema schema) {

        return storage(schema).eventStrategy(schema);
    }

    @Override
    default StorageTraits storageTraits(final Schema schema) {

        return storage(schema).storageTraits(schema);
    }

    @Override
    default Set<Name> supportedExpand(final LinkableSchema schema, final Set<Name> expand) {

        return storage(schema).supportedExpand(schema, expand);
    }

    @Override
    default CompletableFuture<Map<String, Object>> get(final Consistency consistency, final ReferableSchema schema, final String id, final Set<Name> expand) {

        // Explicit non-delegation, default implementation must be called to reach read() override
        return Storage.super.get(consistency, schema, id, expand);
    }

    @Override
    default CompletableFuture<Map<String, Object>> getVersion(final Consistency consistency, final ReferableSchema schema, final String id, final long version, final Set<Name> expand) {

        // Explicit non-delegation, default implementation must be called to reach read() override
        return Storage.super.getVersion(consistency, schema, id, version, expand);
    }

    @Override
    default Pager<Map<String, Object>> query(final Consistency consistency, final QueryableSchema schema, final Map<String, Object> arguments, final Expression query, final List<Sort> sort, final Set<Name> expand) {

        return storage(schema).query(consistency, schema, arguments, query, sort, expand);
    }

    @Override
    default Pager<Map<String, Object>> queryHistory(final Consistency consistency, final ReferableSchema schema, final String id, final Expression query, final List<Sort> sort, final Set<Name> expand) {

        return storage(schema).queryHistory(consistency, schema, id, query, sort, expand);
    }

    @Override
    default CompletableFuture<Set<Event>> afterCreate(final ObjectSchema schema, final String id, final Map<String, Object> after) {

        return storage(schema).afterCreate(schema, id, after);
    }

    @Override
    default CompletableFuture<Set<Event>> afterUpdate(final ObjectSchema schema, final String id, final long version, final Map<String, Object> before, final Map<String, Object> after) {

        return storage(schema).afterUpdate(schema, id, version, before, after);
    }

    @Override
    default CompletableFuture<Set<Event>> afterDelete(final ObjectSchema schema, final String id, final long version, final Map<String, Object> before) {

        return storage(schema).afterDelete(schema, id, version, before);
    }

    @Override
    default CompletableFuture<Long> increment(final SequenceSchema schema) {

        return storage(schema).increment(schema);
    }

    @Override
    default ReadTransaction read(final Consistency consistency) {

        final IdentityHashMap<Storage, ReadTransaction> transactions = new IdentityHashMap<>();
        return new ReadTransaction() {

            public ReadTransaction delegate(final ReferableSchema schema) {

                return transactions.computeIfAbsent(storage(schema), v -> v.read(consistency));
            }

            @Override
            public ReadTransaction get(final ReferableSchema schema, final String id, final Set<Name> expand) {

                delegate(schema).get(schema, id, expand);
                return this;
            }

            @Override
            public ReadTransaction getVersion(final ReferableSchema schema, final String id, final long version, final Set<Name> expand) {

                delegate(schema).getVersion(schema, id, version, expand);
                return this;
            }

            @Override
            public CompletableFuture<BatchResponse> read() {

                return CompletableFutures.allOf(transactions.values().stream().map(ReadTransaction::read))
                        .thenApply(BatchResponse::merge);
            }
        };
    }

    @Override
    default WriteTransaction write(final Consistency consistency, final Versioning versioning) {

        final IdentityHashMap<Storage, WriteTransaction> transactions = new IdentityHashMap<>();
        return new WriteTransaction() {

            public WriteTransaction delegate(final LinkableSchema schema) {

                return transactions.computeIfAbsent(storage(schema), v -> v.write(consistency, versioning));
            }

            @Override
            @Deprecated
            public WriteTransaction write(final LinkableSchema schema, final Map<String, Object> after) {

                delegate(schema).write(schema, after);
                return this;
            }

            @Override
            public WriteTransaction createObject(final ObjectSchema schema, final String id, final Map<String, Object> after) {

                delegate(schema).createObject(schema, id, after);
                return this;
            }

            @Override
            public WriteTransaction updateObject(final ObjectSchema schema, final String id, final Map<String, Object> before, final Map<String, Object> after) {

                delegate(schema).updateObject(schema, id, before, after);
                return this;
            }

            @Override
            public WriteTransaction deleteObject(final ObjectSchema schema, final String id, final Map<String, Object> before) {

                delegate(schema).deleteObject(schema, id, before);
                return this;
            }

            @Override
            public WriteTransaction writeHistory(final ObjectSchema schema, final String id, final Map<String, Object> after) {

                delegate(schema).writeHistory(schema, id, after);
                return this;
            }

            @Override
            public CompletableFuture<BatchResponse> write() {

                if(consistency != Consistency.NONE && transactions.size() > 1) {
                    throw new IllegalStateException("Consistent write transaction spanned multiple storage engines");
                } else {
                    return CompletableFutures.allOf(transactions.values().stream().map(WriteTransaction::write))
                            .thenApply(BatchResponse::merge);
                }
            }
        };
    }
}

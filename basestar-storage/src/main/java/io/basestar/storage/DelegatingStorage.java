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

    Storage storage(LinkableSchema schema);

    @Override
    default void validate(final ObjectSchema schema) {

        storage(schema).validate(schema);
    }

    @Override
    default EventStrategy eventStrategy(final ReferableSchema schema) {

        return storage(schema).eventStrategy(schema);
    }

    @Override
    default StorageTraits storageTraits(final ReferableSchema schema) {

        return storage(schema).storageTraits(schema);
    }

    @Override
    default Set<Name> supportedExpand(final LinkableSchema schema, final Set<Name> expand) {

        return storage(schema).supportedExpand(schema, expand);
    }

    @Override
    default Pager<Map<String, Object>> queryObject(final ObjectSchema schema, final Expression query, final List<Sort> sort, final Set<Name> expand) {

        return storage(schema).queryObject(schema, query, sort, expand);
    }

    @Override
    default Pager<Map<String, Object>> queryInterface(final InterfaceSchema schema, final Expression query, final List<Sort> sort, final Set<Name> expand) {

        return storage(schema).queryInterface(schema, query, sort, expand);
    }

    @Override
    default Pager<Map<String, Object>> queryView(final ViewSchema schema, final Expression query, final List<Sort> sort, final Set<Name> expand) {

        return storage(schema).queryView(schema, query, sort, expand);
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
    default ReadTransaction read(final Consistency consistency) {

        final IdentityHashMap<Storage, ReadTransaction> transactions = new IdentityHashMap<>();
        return new ReadTransaction() {
            @Override
            public ReadTransaction getInterface(final InterfaceSchema schema, final String id, final Set<Name> expand) {

                transactions.computeIfAbsent(storage(schema), v -> v.read(consistency))
                        .getInterface(schema, id, expand);
                return this;
            }

            @Override
            public ReadTransaction getInterfaceVersion(final InterfaceSchema schema, final String id, final long version, final Set<Name> expand) {

                transactions.computeIfAbsent(storage(schema), v -> v.read(consistency))
                        .getInterfaceVersion(schema, id, version, expand);
                return this;
            }

            @Override
            public ReadTransaction getObject(final ObjectSchema schema, final String id, final Set<Name> expand) {

                transactions.computeIfAbsent(storage(schema), v -> v.read(consistency))
                        .getObject(schema, id, expand);
                return this;
            }

            @Override
            public ReadTransaction getObjectVersion(final ObjectSchema schema, final String id, final long version, final Set<Name> expand) {

                transactions.computeIfAbsent(storage(schema), v -> v.read(consistency))
                        .getObjectVersion(schema, id, version, expand);
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
            @Override
            public WriteTransaction createObject(final ObjectSchema schema, final String id, final Map<String, Object> after) {

                transactions.computeIfAbsent(storage(schema), v -> v.write(consistency, versioning))
                        .createObject(schema, id, after);

                return this;
            }

            @Override
            public WriteTransaction updateObject(final ObjectSchema schema, final String id, final Map<String, Object> before, final Map<String, Object> after) {

                transactions.computeIfAbsent(storage(schema), v -> v.write(consistency, versioning))
                        .updateObject(schema, id, before, after);

                return this;
            }

            @Override
            public WriteTransaction deleteObject(final ObjectSchema schema, final String id, final Map<String, Object> before) {

                transactions.computeIfAbsent(storage(schema), v -> v.write(consistency, versioning))
                        .deleteObject(schema, id, before);

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

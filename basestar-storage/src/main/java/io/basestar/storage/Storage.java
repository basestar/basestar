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
import io.basestar.schema.Consistency;
import io.basestar.schema.History;
import io.basestar.schema.Index;
import io.basestar.schema.ObjectSchema;
import io.basestar.schema.exception.UnsupportedConsistencyException;
import io.basestar.storage.util.Pager;
import io.basestar.util.Path;
import io.basestar.util.Sort;
import lombok.RequiredArgsConstructor;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

public interface Storage {

    default void validate(final ObjectSchema schema) {

        final StorageTraits storageTraits = storageTraits(schema);
        final Consistency bestHistoryWrite = storageTraits.getHistoryConsistency();
        final Consistency bestSingleIndexWrite = storageTraits.getSingleValueIndexConsistency();
        final Consistency bestMultiIndexWrite = storageTraits.getMultiValueIndexConsistency();

        final History history = schema.getHistory();
        if(history.isEnabled()) {
            final Consistency requested = history.getConsistency();
            if (requested != null && requested.isStronger(bestHistoryWrite)) {
                throw new UnsupportedConsistencyException(schema.getName() + ".history", name(schema), bestHistoryWrite, requested);
            }
        }
        for(final Map.Entry<String, Index> entry : schema.getIndexes().entrySet()) {
            final Index index = entry.getValue();
            final Consistency requested = index.getConsistency();
            if(requested != null) {
                final Consistency best = index.isMultiValue() ? bestMultiIndexWrite : bestSingleIndexWrite;
                if(requested.isStronger(best)) {
                    throw new UnsupportedConsistencyException(schema.getName() + "." + entry.getKey(), name(schema), best, requested);
                }
            }
        }
    }

    default String name(final ObjectSchema schema) {

        return getClass().getSimpleName();
    }

    CompletableFuture<Map<String, Object>> readObject(ObjectSchema schema, String id);

    CompletableFuture<Map<String, Object>> readObjectVersion(ObjectSchema schema, String id, long version);

    List<Pager.Source<Map<String, Object>>> query(ObjectSchema schema, Expression query, List<Sort> sort);

//    CompletableFuture<Map<String, Object>> aggregate(ObjectSchema schema, Aggregate aggregate);

    default Set<Path> canHandleExpand(final ObjectSchema schema, final Set<Path> expand) {

        return Collections.emptySet();
    }

    ReadTransaction read(Consistency consistency);

    WriteTransaction write(Consistency consistency);

    EventStrategy eventStrategy(ObjectSchema schema);

    StorageTraits storageTraits(ObjectSchema schema);

    interface ReadTransaction {

        ReadTransaction readObject(ObjectSchema schema, String id);

        ReadTransaction readObjectVersion(ObjectSchema schema, String id, long version);

        CompletableFuture<BatchResponse> read();

        // Basic non-consistent read, delegates to non-isolated storage methods

        @RequiredArgsConstructor
        class Basic implements ReadTransaction {

            private final List<Supplier<CompletableFuture<BatchResponse>>> requests = new ArrayList<>();

            private final Storage delegate;

            @Override
            public ReadTransaction readObject(final ObjectSchema schema, final String id) {

                requests.add(() -> delegate.readObject(schema, id)
                        .thenApply(v -> BatchResponse.single(schema.getName(), v)));
                return this;
            }

            @Override
            public ReadTransaction readObjectVersion(final ObjectSchema schema, final String id, final long version) {

                requests.add(() -> delegate.readObjectVersion(schema, id, version)
                        .thenApply(v -> BatchResponse.single(schema.getName(), v)));
                return this;
            }

            @Override
            public CompletableFuture<BatchResponse> read() {

                return BatchResponse.mergeFutures(requests.stream().map(Supplier::get));
            }
        }
    }

    interface WriteTransaction {

        WriteTransaction createObject(ObjectSchema schema, String id, Map<String, Object> after);

        WriteTransaction updateObject(ObjectSchema schema, String id, Map<String, Object> before, Map<String, Object> after);

        WriteTransaction deleteObject(ObjectSchema schema, String id, Map<String, Object> before);

        WriteTransaction createIndex(ObjectSchema schema, Index index, String id, long version, Index.Key key, Map<String, Object> projection);

        WriteTransaction updateIndex(ObjectSchema schema, Index index, String id, long version, Index.Key key, Map<String, Object> projection);

        WriteTransaction deleteIndex(ObjectSchema schema, Index index, String id, long version, Index.Key key);

        WriteTransaction createHistory(ObjectSchema schema, String id, long version, Map<String, Object> after);

        CompletableFuture<BatchResponse> commit();
    }

    enum EventStrategy {

        SUPPRESS,
        EMIT
    }
}

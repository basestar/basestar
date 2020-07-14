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
import io.basestar.schema.*;
import io.basestar.schema.exception.UnsupportedConsistencyException;
import io.basestar.storage.exception.UnsupportedQueryException;
import io.basestar.storage.util.Pager;
import io.basestar.util.Name;
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
                throw new UnsupportedConsistencyException(schema.getQualifiedName() + ".history", name(schema), bestHistoryWrite, requested);
            }
        }
        for(final Map.Entry<String, Index> entry : schema.getIndexes().entrySet()) {
            final Index index = entry.getValue();
            final Consistency requested = index.getConsistency();
            if(requested != null) {
                final Consistency best = index.isMultiValue() ? bestMultiIndexWrite : bestSingleIndexWrite;
                if(requested.isStronger(best)) {
                    throw new UnsupportedConsistencyException(schema.getQualifiedName() + "." + entry.getKey(), name(schema), best, requested);
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

    List<Pager.Source<Map<String, Object>>> aggregate(ObjectSchema schema, Expression query, Map<String, Expression> group, Map<String, Aggregate> aggregates);

    default Set<Name> canHandleExpand(final ObjectSchema schema, final Set<Name> expand) {

        return Collections.emptySet();
    }

    ReadTransaction read(Consistency consistency);

    WriteTransaction write(Consistency consistency);

    EventStrategy eventStrategy(ObjectSchema schema);

    StorageTraits storageTraits(ObjectSchema schema);

    CompletableFuture<?> asyncIndexCreated(ObjectSchema schema, Index index, String id, long version, Index.Key key, Map<String, Object> projection);

    CompletableFuture<?> asyncIndexUpdated(ObjectSchema schema, Index index, String id, long version, Index.Key key, Map<String, Object> projection);

    CompletableFuture<?> asyncIndexDeleted(ObjectSchema schema, Index index, String id, long version, Index.Key key);

    CompletableFuture<?> asyncHistoryCreated(ObjectSchema schema, String id, long version, Map<String, Object> after);

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
                        .thenApply(v -> BatchResponse.single(schema.getQualifiedName(), v)));
                return this;
            }

            @Override
            public ReadTransaction readObjectVersion(final ObjectSchema schema, final String id, final long version) {

                requests.add(() -> delegate.readObjectVersion(schema, id, version)
                        .thenApply(v -> BatchResponse.single(schema.getQualifiedName(), v)));
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

        CompletableFuture<BatchResponse> write();
    }

    enum EventStrategy {

        SUPPRESS,
        EMIT
    }

    interface WithoutWrite extends WithoutWriteIndex, WithoutWriteHistory {

        default WriteTransaction write(final Consistency consistency) {

            throw new UnsupportedOperationException();
        }
    }

    interface WithoutAggregate extends Storage {

        default List<Pager.Source<Map<String, Object>>> aggregate(final ObjectSchema schema, final Expression query, final Map<String, Expression> group, final Map<String, Aggregate> aggregates) {

            throw new UnsupportedQueryException(schema.getQualifiedName(), query);
        }
    }

    interface WithoutQuery extends WithoutAggregate {

        default List<Pager.Source<Map<String, Object>>> query(final ObjectSchema schema, final Expression query, final List<Sort> sort) {

            throw new UnsupportedQueryException(schema.getQualifiedName(), query);
        }
    }

    interface WithoutHistory extends WithoutWriteHistory {

        default CompletableFuture<Map<String, Object>> readObjectVersion(final ObjectSchema schema, final String id, final long version) {

            return readObject(schema, id).thenApply(object -> {
                if (object != null && Long.valueOf(version).equals(Instance.getVersion(object))) {
                    return object;
                } else {
                    return null;
                }
            });
        }
    }

    interface WithoutWriteHistory extends Storage {

        default CompletableFuture<?> asyncHistoryCreated(final ObjectSchema schema, String id, final long version, Map<String, Object> after) {

            throw new UnsupportedOperationException();
        }
    }

    interface WithoutWriteIndex extends Storage {

        default CompletableFuture<?> asyncIndexCreated(final ObjectSchema schema, final Index index, String id, final long version, final Index.Key key, final Map<String, Object> projection) {

            throw new UnsupportedOperationException();
        }

        default CompletableFuture<?> asyncIndexUpdated(final ObjectSchema schema, final Index index, final String id, final long version, final Index.Key key, final Map<String, Object> projection) {

            throw new UnsupportedOperationException();
        }

        default CompletableFuture<?> asyncIndexDeleted(final ObjectSchema schema, final Index index, final String id, final long version, final Index.Key key) {

            throw new UnsupportedOperationException();
        }
    }

    interface WithWriteHistory extends Storage {

        WriteTransaction write(Consistency consistency);

        default CompletableFuture<?> asyncHistoryCreated(final ObjectSchema schema, String id, final long version, Map<String, Object> after) {

            final WriteTransaction write = write(Consistency.ASYNC);
            write.createHistory(schema, id, version, after);
            return write.write();
        }

        interface WriteTransaction extends Storage.WriteTransaction {

            WriteTransaction createHistory(ObjectSchema schema, String id, long version, Map<String, Object> after);
        }
    }

    interface WithWriteIndex extends Storage {

        WriteTransaction write(Consistency consistency);

        default CompletableFuture<?> asyncIndexCreated(final ObjectSchema schema, final Index index, String id, final long version, final Index.Key key, final Map<String, Object> projection) {

            final WriteTransaction write = write(Consistency.ASYNC);
            write.createIndex(schema, index, id, version, key, projection);
            return write.write();
        }

        default CompletableFuture<?> asyncIndexUpdated(final ObjectSchema schema, final Index index, final String id, final long version, final Index.Key key, final Map<String, Object> projection) {

            final WriteTransaction write = write(Consistency.ASYNC);
            write.updateIndex(schema, index, id, version, key, projection);
            return write.write();
        }

        default CompletableFuture<?> asyncIndexDeleted(final ObjectSchema schema, final Index index, final String id, final long version, final Index.Key key) {

            final WriteTransaction write = write(Consistency.ASYNC);
            write.deleteIndex(schema, index, id, version, key);
            return write.write();
        }

        interface WriteTransaction extends Storage.WriteTransaction {

            WriteTransaction createIndex(ObjectSchema schema, Index index, String id, long version, Index.Key key, Map<String, Object> projection);

            WriteTransaction updateIndex(ObjectSchema schema, Index index, String id, long version, Index.Key key, Map<String, Object> projection);

            WriteTransaction deleteIndex(ObjectSchema schema, Index index, String id, long version, Index.Key key);
        }
    }
}

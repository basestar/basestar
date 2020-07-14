package io.basestar.storage;

import io.basestar.expression.Expression;
import io.basestar.expression.aggregate.Aggregate;
import io.basestar.schema.Concurrency;
import io.basestar.schema.Consistency;
import io.basestar.schema.ObjectSchema;
import io.basestar.storage.util.Pager;
import io.basestar.util.PagedList;
import io.basestar.util.Sort;
import lombok.RequiredArgsConstructor;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@RequiredArgsConstructor
public class NullStorage implements Storage.WithoutWriteIndex, Storage.WithoutWriteHistory {

    private final EventStrategy eventStrategy;

    @Override
    public CompletableFuture<Map<String, Object>> readObject(final ObjectSchema schema, final String id) {

        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Map<String, Object>> readObjectVersion(final ObjectSchema schema, final String id, final long version) {

        return CompletableFuture.completedFuture(null);
    }

    @Override
    public List<Pager.Source<Map<String, Object>>> query(final ObjectSchema schema, final Expression query, final List<Sort> sort) {

        return Collections.singletonList((count, pagingToken, stats) -> CompletableFuture.completedFuture(PagedList.empty()));
    }

    @Override
    public List<Pager.Source<Map<String, Object>>> aggregate(final ObjectSchema schema, final Expression query, final Map<String, Expression> group, final Map<String, Aggregate> aggregates) {

        return Collections.singletonList((count, pagingToken, stats) -> CompletableFuture.completedFuture(PagedList.empty()));
    }

    @Override
    public ReadTransaction read(final Consistency consistency) {

        return new ReadTransaction.Basic(this);
    }

    @Override
    public WriteTransaction write(final Consistency consistency) {

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

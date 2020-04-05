package io.basestar.storage;

import io.basestar.expression.Expression;
import io.basestar.schema.Consistency;
import io.basestar.schema.Index;
import io.basestar.schema.ObjectSchema;
import io.basestar.storage.util.Pager;
import io.basestar.util.Sort;

import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public interface DelegatingStorage extends Storage {

    Storage storage(ObjectSchema schema);

    @Override
    default void validate(final ObjectSchema schema) {

        storage(schema).validate(schema);
    }

    @Override
    default String name(final ObjectSchema schema) {

        return storage(schema).name(schema);
    }

    @Override
    default CompletableFuture<Map<String, Object>> readObject(final ObjectSchema schema, final String id) {

        return storage(schema).readObject(schema, id);
    }

    @Override
    default CompletableFuture<Map<String, Object>> readObjectVersion(final ObjectSchema schema, final String id, final long version) {

        return storage(schema).readObjectVersion(schema, id, version);
    }

    @Override
    default List<Pager.Source<Map<String, Object>>> query(final ObjectSchema schema, final Expression query, final List<Sort> sort) {

        return storage(schema).query(schema, query, sort);
    }

    @Override
    default EventStrategy eventStrategy(final ObjectSchema schema) {

        return storage(schema).eventStrategy(schema);
    }

    @Override
    default StorageTraits storageTraits(final ObjectSchema schema) {

        return storage(schema).storageTraits(schema);
    }

    @Override
    default ReadTransaction read(final Consistency consistency) {

        final IdentityHashMap<Storage, ReadTransaction> transactions = new IdentityHashMap<>();
        return new ReadTransaction() {
            @Override
            public ReadTransaction readObject(final ObjectSchema schema, final String id) {

                transactions.computeIfAbsent(storage(schema), v -> v.read(consistency))
                        .readObject(schema, id);

                return this;
            }

            @Override
            public ReadTransaction readObjectVersion(final ObjectSchema schema, final String id, final long version) {

                transactions.computeIfAbsent(storage(schema), v -> v.read(consistency))
                        .readObjectVersion(schema, id, version);

                return this;
            }

            @Override
            public CompletableFuture<BatchResponse> read() {

                return BatchResponse.mergeFutures(transactions.values().stream().map(ReadTransaction::read));

//                final List<CompletableFuture<BatchResponse>> futures = batches.values().stream()
//                        .map(ReadTransaction::read).collect(Collectors.toList());
//
//                return CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[0]))
//                        .thenApply(ignored -> new BatchResponse.Basic(futures.stream()
//                                .flatMap(future -> future.getNow(BatchResponse.empty()).stream())
//                                .collect(Collectors.toList())));
            }
        };
    }

    @Override
    default WriteTransaction write(final Consistency consistency) {

        final IdentityHashMap<Storage, WriteTransaction> transactions = new IdentityHashMap<>();
        return new WriteTransaction() {
            @Override
            public WriteTransaction createObject(final ObjectSchema schema, final String id, final Map<String, Object> after) {

                transactions.computeIfAbsent(storage(schema), v -> v.write(consistency))
                        .createObject(schema, id, after);

                return this;
            }

            @Override
            public WriteTransaction updateObject(final ObjectSchema schema, final String id, final Map<String, Object> before, final Map<String, Object> after) {

                transactions.computeIfAbsent(storage(schema), v -> v.write(consistency))
                        .updateObject(schema, id, before, after);

                return this;
            }

            @Override
            public WriteTransaction deleteObject(final ObjectSchema schema, final String id, final Map<String, Object> before) {

                transactions.computeIfAbsent(storage(schema), v -> v.write(consistency))
                        .deleteObject(schema, id, before);

                return this;
            }

            @Override
            public WriteTransaction createIndex(final ObjectSchema schema, final Index index, final String id, final long version, final Index.Key key, final Map<String, Object> projection) {

                transactions.computeIfAbsent(storage(schema), v -> v.write(consistency))
                        .createIndex(schema, index, id, version, key, projection);

                return this;
            }

            @Override
            public WriteTransaction updateIndex(final ObjectSchema schema, final Index index, final String id, final long version, final Index.Key key, final Map<String, Object> projection) {

                transactions.computeIfAbsent(storage(schema), v -> v.write(consistency))
                        .updateIndex(schema, index, id, version, key, projection);

                return this;
            }

            @Override
            public WriteTransaction deleteIndex(final ObjectSchema schema, final Index index, final String id, final long version, final Index.Key key) {

                transactions.computeIfAbsent(storage(schema), v -> v.write(consistency))
                        .deleteIndex(schema, index, id, version, key);

                return this;
            }

            @Override
            public WriteTransaction createHistory(final ObjectSchema schema, final String id, final long version, final Map<String, Object> after) {

                transactions.computeIfAbsent(storage(schema), v -> v.write(consistency))
                        .createHistory(schema, id, version, after);

                return this;
            }

            @Override
            public CompletableFuture<BatchResponse> commit() {

                if(consistency != Consistency.NONE && transactions.size() > 1) {
                    throw new IllegalStateException("Consistent write transaction spanned multiple storage engines");
                } else {

                    return BatchResponse.mergeFutures(transactions.values().stream().map(WriteTransaction::commit));
//
//                    final List<CompletableFuture<BatchResponse>> futures = transactions.values().stream()
//                            .map(WriteTransaction::commit)
//                            .collect(Collectors.toList());
//
//                    return CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[0]))
//                            .thenApply(ignored -> {
//                                final Set<Map<String, Object>> results = futures.stream()
//                                        .flatMap(v -> v.getNow(BatchResponse.empty()).stream())
//                                        .collect(Collectors.toSet());
//                                return new BatchResponse.Basic(results);
//                            });
                }
            }
        };
    }
}

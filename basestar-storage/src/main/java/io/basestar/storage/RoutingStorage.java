//package io.basestar.storage;
//
//import io.basestar.expression.Expression;
//import io.basestar.expression.aggregate.Aggregate;
//import io.basestar.schema.Consistency;
//import io.basestar.schema.Index;
//import io.basestar.schema.ObjectSchema;
//import io.basestar.storage.util.Pager;
//import io.basestar.util.Name;
//import io.basestar.util.Sort;
//
//import java.util.IdentityHashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.concurrent.CompletableFuture;
//
//public interface RoutingStorage extends Storage {
//
//    Storage storage(Name namespace, ObjectSchema relativeSchema);
//
//    default Name namespace(final ObjectSchema absoluteSchema) {
//
//        final Name name = absoluteSchema.getQualifiedName();
//        if(name.size() > 1) {
//            return Name.of(name.first());
//        } else {
//            return Name.empty();
//        }
//    }
//
//    default ObjectSchema relative(final Name namespace, final ObjectSchema absoluteSchema) {
//
//    }
//
//    default Map<String, Object> absolute(final Name namespace, final ObjectSchema relativeSchema, final Map<String, Object> object) {
//
//    }
//
//    default List<Pager.Source<Map<String, Object>>> absolute(final Name namespace, final ObjectSchema relativeSchema, final List<Pager.Source<Map<String, Object>>> sources) {
//
//    }
//
//    @Override
//    default void validate(final ObjectSchema absoluteSchema) {
//
//        final Name namespace = namespace(absoluteSchema);
//        final ObjectSchema relativeSchema = relative(namespace, absoluteSchema);
//        storage(namespace, relativeSchema).validate(relative(namespace, relativeSchema));
//    }
//
//    @Override
//    default String name(final ObjectSchema absoluteSchema) {
//
//        final Name namespace = namespace(absoluteSchema);
//        final ObjectSchema relativeSchema = relative(namespace, absoluteSchema);
//        return storage(namespace, relativeSchema).name(relative(namespace, relativeSchema));
//    }
//
//    @Override
//    default CompletableFuture<Map<String, Object>> readObject(final ObjectSchema absoluteSchema, final String id) {
//
//        final Name namespace = namespace(absoluteSchema);
//        final ObjectSchema relativeSchema = relative(namespace, absoluteSchema);
//        return storage(namespace, relativeSchema).readObject(relative(namespace, relativeSchema), id)
//                .thenApply(result -> absolute(namespace, relativeSchema, result));
//    }
//
//    @Override
//    default List<Pager.Source<Map<String, Object>>> aggregate(final ObjectSchema absoluteSchema, final Expression query, final Map<String, Expression> group, final Map<String, Aggregate> aggregates) {
//
//        final Name namespace = namespace(absoluteSchema);
//        final ObjectSchema relativeSchema = relative(namespace, absoluteSchema);
//        return absolute(namespace, relativeSchema, storage(namespace, relativeSchema).aggregate(relative(namespace, relativeSchema), query, group, aggregates));
//    }
//
//    @Override
//    default CompletableFuture<Map<String, Object>> readObjectVersion(final ObjectSchema absoluteSchema, final String id, final long version) {
//
//        final Name namespace = namespace(absoluteSchema);
//        final ObjectSchema relativeSchema = relative(namespace, absoluteSchema);
//        return storage(namespace, relativeSchema).readObjectVersion(relative(namespace, relativeSchema), id, version)
//                .thenApply(result -> absolute(namespace, relativeSchema, result));
//    }
//
//    @Override
//    default List<Pager.Source<Map<String, Object>>> query(final ObjectSchema absoluteSchema, final Expression query, final List<Sort> sort) {
//
//        final Name namespace = namespace(absoluteSchema);
//        final ObjectSchema relativeSchema = relative(namespace, absoluteSchema);
//        return absolute(namespace, relativeSchema, storage(namespace, relativeSchema).query(relative(namespace, relativeSchema), query, sort));
//    }
//
//    @Override
//    default Storage.EventStrategy eventStrategy(final ObjectSchema absoluteSchema) {
//
//        final Name namespace = namespace(absoluteSchema);
//        final ObjectSchema relativeSchema = relative(namespace, absoluteSchema);
//        return storage(namespace, relativeSchema).eventStrategy(relativeSchema);
//    }
//
//    @Override
//    default StorageTraits storageTraits(final ObjectSchema absoluteSchema) {
//
//        final Name namespace = namespace(absoluteSchema);
//        final ObjectSchema relativeSchema = relative(namespace, absoluteSchema);
//        return storage(namespace, relativeSchema).storageTraits(relativeSchema);
//    }
//
//    @Override
//    default Storage.ReadTransaction read(final Consistency consistency) {
//
//        final IdentityHashMap<Storage, Storage.ReadTransaction> transactions = new IdentityHashMap<>();
//        return new Storage.ReadTransaction() {
//            @Override
//            public Storage.ReadTransaction readObject(final ObjectSchema absoluteSchema, final String id) {
//
//                final Name namespace = namespace(absoluteSchema);
//                final ObjectSchema relativeSchema = relative(namespace, absoluteSchema);
//                transactions.computeIfAbsent(storage(namespace, relativeSchema), v -> v.read(consistency))
//                        .readObject(relativeSchema, id);
//
//                return this;
//            }
//
//            @Override
//            public Storage.ReadTransaction readObjectVersion(final ObjectSchema absoluteSchema, final String id, final long version) {
//
//                final Name namespace = namespace(absoluteSchema);
//                final ObjectSchema relativeSchema = relative(namespace, absoluteSchema);
//                transactions.computeIfAbsent(storage(namespace, relativeSchema), v -> v.read(consistency))
//                        .readObjectVersion(namespace, relativeSchema, id, version);
//
//                return this;
//            }
//
//            @Override
//            public CompletableFuture<BatchResponse> read() {
//
//                return BatchResponse.mergeFutures(transactions.values().stream().map(Storage.ReadTransaction::read));
//            }
//        };
//    }
//
//    @Override
//    default Storage.WriteTransaction write(final Consistency consistency) {
//
//        final IdentityHashMap<Storage, Storage.WriteTransaction> transactions = new IdentityHashMap<>();
//        return new Storage.WriteTransaction() {
//            @Override
//            public Storage.WriteTransaction createObject(final ObjectSchema absoluteSchema, final String id, final Map<String, Object> after) {
//
//                final Name namespace = namespace(absoluteSchema);
//                final ObjectSchema relativeSchema = relative(namespace, absoluteSchema);
//                transactions.computeIfAbsent(storage(namespace, relativeSchema), v -> v.write(consistency))
//                        .createObject(schema, id, after);
//
//                return this;
//            }
//
//            @Override
//            public Storage.WriteTransaction updateObject(final ObjectSchema absoluteSchema, final String id, final Map<String, Object> before, final Map<String, Object> after) {
//
//                final Name namespace = namespace(absoluteSchema);
//                final ObjectSchema relativeSchema = relative(namespace, absoluteSchema);
//                transactions.computeIfAbsent(storage(namespace, relativeSchema), v -> v.write(consistency))
//                        .updateObject(schema, id, before, after);
//
//                return this;
//            }
//
//            @Override
//            public Storage.WriteTransaction deleteObject(final ObjectSchema absoluteSchema, final String id, final Map<String, Object> before) {
//
//                final Name namespace = namespace(absoluteSchema);
//                final ObjectSchema relativeSchema = relative(namespace, absoluteSchema);
//                transactions.computeIfAbsent(storage(namespace, relativeSchema), v -> v.write(consistency))
//                        .deleteObject(schema, id, before);
//
//                return this;
//            }
//
//            @Override
//            public Storage.WriteTransaction createIndex(final ObjectSchema absoluteSchema, final Index index, final String id, final long version, final Index.Key key, final Map<String, Object> projection) {
//
//                final Name namespace = namespace(absoluteSchema);
//                final ObjectSchema relativeSchema = relative(namespace, absoluteSchema);
//                transactions.computeIfAbsent(storage(namespace, relativeSchema), v -> v.write(consistency))
//                        .createIndex(schema, index, id, version, key, projection);
//
//                return this;
//            }
//
//            @Override
//            public Storage.WriteTransaction updateIndex(final ObjectSchema absoluteSchema, final Index index, final String id, final long version, final Index.Key key, final Map<String, Object> projection) {
//
//                final Name namespace = namespace(absoluteSchema);
//                final ObjectSchema relativeSchema = relative(namespace, absoluteSchema);
//                transactions.computeIfAbsent(storage(namespace, relativeSchema), v -> v.write(consistency))
//                        .updateIndex(schema, index, id, version, key, projection);
//
//                return this;
//            }
//
//            @Override
//            public Storage.WriteTransaction deleteIndex(final ObjectSchema absoluteSchema, final Index index, final String id, final long version, final Index.Key key) {
//
//                final Name namespace = namespace(absoluteSchema);
//                final ObjectSchema relativeSchema = relative(namespace, absoluteSchema);
//                transactions.computeIfAbsent(storage(namespace, relativeSchema), v -> v.write(consistency))
//                        .deleteIndex(schema, index, id, version, key);
//
//                return this;
//            }
//
//            @Override
//            public Storage.WriteTransaction createHistory(final ObjectSchema absoluteSchema, final String id, final long version, final Map<String, Object> after) {
//
//                final Name namespace = namespace(absoluteSchema);
//                final ObjectSchema relativeSchema = relative(namespace, absoluteSchema);
//                transactions.computeIfAbsent(storage(namespace, relativeSchema), v -> v.write(consistency))
//                        .createHistory(schema, id, version, after);
//
//                return this;
//            }
//
//            @Override
//            public CompletableFuture<BatchResponse> commit() {
//
//                if(consistency != Consistency.NONE && transactions.size() > 1) {
//                    throw new IllegalStateException("Consistent write transaction spanned multiple storage engines");
//                } else {
//
//                    return BatchResponse.mergeFutures(transactions.values().stream().map(Storage.WriteTransaction::commit));
//                }
//            }
//        };
//    }
//
//}

package io.basestar.storage;

import io.basestar.expression.Expression;
import io.basestar.expression.aggregate.Aggregate;
import io.basestar.schema.Consistency;
import io.basestar.schema.Index;
import io.basestar.schema.Namespace;
import io.basestar.schema.ObjectSchema;
import io.basestar.storage.util.Pager;
import io.basestar.util.Name;
import io.basestar.util.Sort;

import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public interface NamespacedStorage extends Storage {

    Storage storage(Name namespace, ObjectSchema relativeSchema);

    default Name namespace(final ObjectSchema absoluteSchema) {

        final Name name = absoluteSchema.getQualifiedName();
        if(name.size() > 1) {
            return Name.of(name.first());
        } else {
            return Name.empty();
        }
    }

    default ObjectSchema relative(final Name namespace, final ObjectSchema absoluteSchema) {

        final Name absoluteName = absoluteSchema.getQualifiedName();
        assert namespace.isParent(absoluteName);
        final Name relativeName = absoluteName.range(absoluteName.size());
        final Namespace absoluteNamespace = Namespace.from(absoluteSchema.dependencies());
        final Namespace relativeNamespace = absoluteNamespace.relative(namespace);
        return relativeNamespace.requireObjectSchema(relativeName);
    }

    default Map<String, Object> relative(final Name namespace, final ObjectSchema absoluteSchema, final Map<String, Object> absoluteObject) {

        return null;//absoluteSchema.relative(namespace, absoluteObject);
    }

    default Map<String, Object> absolute(final Name namespace, final ObjectSchema relativeSchema, final Map<String, Object> relativeObject) {

        return null;//relativeSchema.absolute(namespace, relativeObject);
    }

    default List<Pager.Source<Map<String, Object>>> absolute(final Name namespace, final ObjectSchema relativeSchema, final List<Pager.Source<Map<String, Object>>> relativeSources) {

        return relativeSources.stream().map(source -> source.map(object -> absolute(namespace, relativeSchema, object))).collect(Collectors.toList());
    }

    default BatchResponse absolute(final Name namespace, final ObjectSchema relativeSchema, final BatchResponse relativeResponse) {

        return new BatchResponse.Basic(relativeResponse.entrySet().stream().collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> absolute(namespace, relativeSchema, entry.getValue())
        )));
    }

    @Override
    default void validate(final ObjectSchema absoluteSchema) {

        final Name namespace = namespace(absoluteSchema);
        final ObjectSchema relativeSchema = relative(namespace, absoluteSchema);
        storage(namespace, relativeSchema)
                .validate(relative(namespace, relativeSchema));
    }

    @Override
    default CompletableFuture<Map<String, Object>> readObject(final ObjectSchema absoluteSchema, final String id) {

        final Name namespace = namespace(absoluteSchema);
        final ObjectSchema relativeSchema = relative(namespace, absoluteSchema);
        return storage(namespace, relativeSchema)
                .readObject(relative(namespace, relativeSchema), id)
                .thenApply(result -> absolute(namespace, relativeSchema, result));
    }

    @Override
    default List<Pager.Source<Map<String, Object>>> aggregate(final ObjectSchema absoluteSchema, final Expression query, final Map<String, Expression> group, final Map<String, Aggregate> aggregates) {

        final Name namespace = namespace(absoluteSchema);
        final ObjectSchema relativeSchema = relative(namespace, absoluteSchema);
        return absolute(namespace, relativeSchema, storage(namespace, relativeSchema)
                .aggregate(relative(namespace, relativeSchema), query, group, aggregates));
    }

    @Override
    default CompletableFuture<Map<String, Object>> readObjectVersion(final ObjectSchema absoluteSchema, final String id, final long version) {

        final Name namespace = namespace(absoluteSchema);
        final ObjectSchema relativeSchema = relative(namespace, absoluteSchema);
        return storage(namespace, relativeSchema)
                .readObjectVersion(relative(namespace, relativeSchema), id, version)
                .thenApply(result -> absolute(namespace, relativeSchema, result));
    }

    @Override
    default List<Pager.Source<Map<String, Object>>> query(final ObjectSchema absoluteSchema, final Expression query, final List<Sort> sort) {

        final Name namespace = namespace(absoluteSchema);
        final ObjectSchema relativeSchema = relative(namespace, absoluteSchema);
        return absolute(namespace, relativeSchema, storage(namespace, relativeSchema)
                .query(relative(namespace, relativeSchema), query, sort));
    }

    @Override
    default Storage.EventStrategy eventStrategy(final ObjectSchema absoluteSchema) {

        final Name namespace = namespace(absoluteSchema);
        final ObjectSchema relativeSchema = relative(namespace, absoluteSchema);
        return storage(namespace, relativeSchema)
                .eventStrategy(relativeSchema);
    }

    @Override
    default StorageTraits storageTraits(final ObjectSchema absoluteSchema) {

        final Name namespace = namespace(absoluteSchema);
        final ObjectSchema relativeSchema = relative(namespace, absoluteSchema);
        return storage(namespace, relativeSchema)
                .storageTraits(relativeSchema);
    }

    @Override
    default CompletableFuture<?> asyncIndexCreated(final ObjectSchema absoluteSchema, final Index index, final String id, final long version, final Index.Key key, final Map<String, Object> absoluteProjection) {

        final Name namespace = namespace(absoluteSchema);
        final ObjectSchema relativeSchema = relative(namespace, absoluteSchema);
        final Map<String, Object> relativeProjection = relative(namespace, absoluteSchema, absoluteProjection);
        return storage(namespace, relativeSchema)
                .asyncIndexCreated(relativeSchema, index, id, version, key, relativeProjection);
    }

    @Override
    default CompletableFuture<?> asyncIndexUpdated(final ObjectSchema absoluteSchema, final Index index, final String id, final long version, final Index.Key key, final Map<String, Object> absoluteProjection) {

        final Name namespace = namespace(absoluteSchema);
        final ObjectSchema relativeSchema = relative(namespace, absoluteSchema);
        final Map<String, Object> relativeProjection = relative(namespace, absoluteSchema, absoluteProjection);
        return storage(namespace, relativeSchema)
                .asyncIndexUpdated(relativeSchema, index, id, version, key, relativeProjection);
    }

    @Override
    default CompletableFuture<?> asyncIndexDeleted(final ObjectSchema absoluteSchema, final Index index, final String id, final long version, final Index.Key key) {

        final Name namespace = namespace(absoluteSchema);
        final ObjectSchema relativeSchema = relative(namespace, absoluteSchema);
        return storage(namespace, relativeSchema)
                .asyncIndexDeleted(relativeSchema, index, id, version, key);
    }

    @Override
    default CompletableFuture<?> asyncHistoryCreated(final ObjectSchema absoluteSchema, final String id, final long version, final Map<String, Object> absoluteAfter) {

        final Name namespace = namespace(absoluteSchema);
        final ObjectSchema relativeSchema = relative(namespace, absoluteSchema);
        final Map<String, Object> relativeAfter = relative(namespace, absoluteSchema, absoluteAfter);
        return storage(namespace, relativeSchema)
                .asyncHistoryCreated(relativeSchema, id, version, relativeAfter);
    }

    @Override
    default Storage.ReadTransaction read(final Consistency consistency) {

        final IdentityHashMap<Storage, Map<Name, Storage.ReadTransaction>> transactions = new IdentityHashMap<>();
        return new Storage.ReadTransaction() {

            private Storage.ReadTransaction transaction(final Name namespace, final ObjectSchema relativeSchema) {

                final Storage storage = storage(namespace, relativeSchema);
                return transactions.computeIfAbsent(storage, ignored -> new HashMap<>())
                                .computeIfAbsent(namespace, ignored -> {
                                    final Storage.ReadTransaction delegate = storage.read(consistency);
                                    return new Storage.ReadTransaction() {

                                        @Override
                                        public ReadTransaction readObject(final ObjectSchema schema, final String id) {

                                            return delegate.readObject(schema, id);
                                        }

                                        @Override
                                        public ReadTransaction readObjectVersion(final ObjectSchema schema, final String id, final long version) {

                                            return delegate.readObjectVersion(schema, id, version);
                                        }

                                        @Override
                                        public CompletableFuture<BatchResponse> read() {

                                            return delegate.read()
                                                    .thenApply(response -> absolute(namespace, relativeSchema, response));
                                        }
                                    };
                                });
            }

            @Override
            public Storage.ReadTransaction readObject(final ObjectSchema absoluteSchema, final String id) {

                final Name namespace = namespace(absoluteSchema);
                final ObjectSchema relativeSchema = relative(namespace, absoluteSchema);
                transaction(namespace, relativeSchema).readObject(relativeSchema, id);
                return this;
            }

            @Override
            public Storage.ReadTransaction readObjectVersion(final ObjectSchema absoluteSchema, final String id, final long version) {

                final Name namespace = namespace(absoluteSchema);
                final ObjectSchema relativeSchema = relative(namespace, absoluteSchema);
                transaction(namespace, relativeSchema).readObjectVersion(relativeSchema, id, version);
                return this;
            }

            @Override
            public CompletableFuture<BatchResponse> read() {

                return BatchResponse.mergeFutures(transactions.values().stream().flatMap(v -> v.values().stream())
                        .map(Storage.ReadTransaction::read));
            }
        };
    }

    @Override
    default Storage.WriteTransaction write(final Consistency consistency) {

        final IdentityHashMap<Storage, Map<Name, Storage.WriteTransaction>> transactions = new IdentityHashMap<>();
        return new Storage.WriteTransaction() {

            private Storage.WriteTransaction transaction(final Name namespace, final ObjectSchema relativeSchema) {

                final Storage storage = storage(namespace, relativeSchema);
                return transactions.computeIfAbsent(storage, ignored -> new HashMap<>())
                        .computeIfAbsent(namespace, ignored -> {
                            final Storage.WriteTransaction delegate = storage.write(consistency);
                            return new Storage.WriteTransaction() {

                                @Override
                                public WriteTransaction createObject(final ObjectSchema schema, final String id, final Map<String, Object> after) {

                                    return delegate.createObject(schema, id, after);
                                }

                                @Override
                                public WriteTransaction updateObject(final ObjectSchema schema, final String id, final Map<String, Object> before, final Map<String, Object> after) {

                                    return delegate.updateObject(schema, id, before, after);
                                }

                                @Override
                                public WriteTransaction deleteObject(final ObjectSchema schema, final String id, final Map<String, Object> before) {

                                    return delegate.deleteObject(schema, id, before);
                                }

                                @Override
                                public CompletableFuture<BatchResponse> write() {

                                    return delegate.write()
                                            .thenApply(response -> absolute(namespace, relativeSchema, response));
                                }
                            };
                        });
            }

            @Override
            public Storage.WriteTransaction createObject(final ObjectSchema absoluteSchema, final String id, final Map<String, Object> absoluteAfter) {

                final Name namespace = namespace(absoluteSchema);
                final ObjectSchema relativeSchema = relative(namespace, absoluteSchema);
                final Map<String, Object> relativeAfter = relative(namespace, absoluteSchema, absoluteAfter);
                transaction(namespace, relativeSchema).createObject(relativeSchema, id, relativeAfter);
                return this;
            }

            @Override
            public Storage.WriteTransaction updateObject(final ObjectSchema absoluteSchema, final String id, final Map<String, Object> absoluteBefore, final Map<String, Object> absoluteAfter) {

                final Name namespace = namespace(absoluteSchema);
                final ObjectSchema relativeSchema = relative(namespace, absoluteSchema);
                final Map<String, Object> relativeABefore = relative(namespace, absoluteSchema, absoluteBefore);
                final Map<String, Object> relativeAfter = relative(namespace, absoluteSchema, absoluteAfter);
                transaction(namespace, relativeSchema).updateObject(relativeSchema, id, relativeABefore, relativeAfter);
                return this;
            }

            @Override
            public Storage.WriteTransaction deleteObject(final ObjectSchema absoluteSchema, final String id, final Map<String, Object> absoluteBefore) {

                final Name namespace = namespace(absoluteSchema);
                final ObjectSchema relativeSchema = relative(namespace, absoluteSchema);
                final Map<String, Object> relativeBefore = relative(namespace, absoluteSchema, absoluteBefore);
                transaction(namespace, relativeSchema).deleteObject(relativeSchema, id, relativeBefore);
                return this;
            }

            @Override
            public CompletableFuture<BatchResponse> write() {

                if(consistency != Consistency.NONE && transactions.size() > 1) {
                    throw new IllegalStateException("Consistent write transaction spanned multiple storage engines");
                } else {
                    return BatchResponse.mergeFutures(transactions.values().stream().flatMap(v -> v.values().stream())
                            .map(Storage.WriteTransaction::write));
                }
            }
        };
    }
}

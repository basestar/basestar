package io.basestar.storage;

import com.google.common.collect.Sets;
import io.basestar.event.Event;
import io.basestar.expression.Expression;
import io.basestar.schema.*;
import io.basestar.util.Name;
import io.basestar.util.Pager;
import io.basestar.util.Sort;

import java.util.*;
import java.util.concurrent.CompletableFuture;

public interface DefaultLayeredStorage extends ValidatingStorage {

    @Override
    ReadTransaction read(Consistency consistency);

    @Override
    WriteTransaction write(Consistency consistency, Versioning versioning);

    @Override
    default Set<Name> supportedExpand(final LinkableSchema schema, final Set<Name> expand) {

        return Sets.intersection(expand, schema.getExpand());
    }

//    @Override
//    @Deprecated
//    default Pager<RepairInfo> repair(final LinkableSchema schema) {
//
//        return (stats, token, count) -> CompletableFuture.completedFuture(Page.empty());
//    }
//
//    @Override
//    @Deprecated
//    default Pager<RepairInfo> repairIndex(final LinkableSchema schema, final Index index) {
//
//        return (stats, token, count) -> CompletableFuture.completedFuture(Page.empty());
//    }

    static boolean hasAsyncHistory(final StorageTraits traits, final ObjectSchema schema) {

        return traits.supportsHistory() && schema.getHistory().isEnabled() && schema.getHistory().getConsistency(traits.getHistoryConsistency()).isAsync();
    }

    static boolean hasSyncHistory(final StorageTraits traits, final ObjectSchema schema) {

        return traits.supportsHistory() && schema.getHistory().isEnabled() && !schema.getHistory().getConsistency(traits.getHistoryConsistency()).isAsync();
    }

    @Override
    default CompletableFuture<Set<Event>> afterCreate(final ObjectSchema schema, final String id, final Map<String, Object> after) {

        final StorageTraits traits = storageTraits(schema);
        if(hasAsyncHistory(traits, schema)) {
            return write(Consistency.ATOMIC, Versioning.CHECKED)
                    .writeHistory(schema, id, after)
                    .write().thenApply(ignored -> Collections.emptySet());
        } else {
            return CompletableFuture.completedFuture(Collections.emptySet());
        }
    }

    @Override
    default CompletableFuture<Set<Event>> afterUpdate(final ObjectSchema schema, final String id, final long version, final Map<String, Object> before, final Map<String, Object> after) {

        final StorageTraits traits = storageTraits(schema);
        if(hasAsyncHistory(traits, schema)) {
            return write(Consistency.ATOMIC, Versioning.CHECKED)
                    .writeHistory(schema, id, after)
                    .write().thenApply(ignored -> Collections.emptySet());
        } else {
            return CompletableFuture.completedFuture(Collections.emptySet());
        }
    }

    @Override
    default CompletableFuture<Set<Event>> afterDelete(final ObjectSchema schema, final String id, final long version, final Map<String, Object> before) {

        return CompletableFuture.completedFuture(Collections.emptySet());
    }

    @Override
    default Pager<Map<String, Object>> queryInterface(final InterfaceSchema schema, final Expression query, final List<Sort> sort, final Set<Name> expand) {

        final Collection<ObjectSchema> objectSchemas = schema.getConcreteExtended();
        final Map<String, Pager<Map<String, Object>>> pagers = new HashMap<>();
        objectSchemas.forEach(objectSchema -> {
            pagers.put(objectSchema.getQualifiedName().toString(), queryObject(objectSchema, query, sort, expand));
        });
        return Pager.merge(Instance.comparator(sort), pagers);


//        return new Storage.ReadTransaction.Delegating() {
//
//            @Override
//            public Storage.ReadTransaction delegate() {
//
//                return delegate;
//            }
//
//            @Override
//            public CompletableFuture<BatchResponse> read() {
//
//                return delegate.read().thenApply(result -> {
//                    final Map<String, Page<Map<String, Object>>> pages = new HashMap<>();
//                    objectSchemas.forEach(objectSchema -> {
//                        final Page.Token objectToken = tokens.get(objectSchema.getQualifiedName().toString());
//                        final Page.OffsetToken offsetToken = Page.OffsetToken.fromToken(objectToken);
//                        if (!offsetToken.isComplete()) {
//                            final Page<Map<String, Object>> page = result.query(objectSchema, query, sort, offsetToken.getToken());
//                            pages.put(objectSchema.getQualifiedName().toString(), page);
//                        }
//                    });
//                    final Page<Map<String, Object>> page = Page.merge(pages, Instance.comparator(sort), count);
//                    return result.withQuery(schema, query, sort, token, page);
//                });
//            }
//        };
    }

    @Override
    default Pager<Map<String, Object>> queryView(final ViewSchema schema, final Expression query, final List<Sort> sort, final Set<Name> expand) {

        throw new UnsupportedOperationException();
    }

    interface ReadTransaction extends Storage.ReadTransaction {

        @Override
        default Storage.ReadTransaction getInterface(final InterfaceSchema schema, final String id, final Set<Name> expand) {

            final Storage.ReadTransaction delegate = this;
            final Collection<ObjectSchema> objectSchemas = schema.getConcreteExtended();
            objectSchemas.forEach(object -> delegate.getObject(object, id, expand));
            return new Storage.ReadTransaction.Delegating() {

                @Override
                public Storage.ReadTransaction delegate() {

                    return delegate;
                }

                @Override
                public CompletableFuture<BatchResponse> read() {

                    return delegate.read().thenApply(result -> {
                        for (final ObjectSchema objectSchema : objectSchemas) {
                            final Map<String, Object> match = result.get(objectSchema, id);
                            if (match != null) {
                                return result.with(schema, match);
                            }
                        }
                        return result;
                    });
                }
            };
        }

        @Override
        default Storage.ReadTransaction getInterfaceVersion(final InterfaceSchema schema, final String id, final long version, final Set<Name> expand) {

            final Storage.ReadTransaction delegate = this;
            final Collection<ObjectSchema> objectSchemas = schema.getConcreteExtended();
            objectSchemas.forEach(object -> delegate.getObjectVersion(object, id, version, expand));
            return new Storage.ReadTransaction.Delegating() {

                @Override
                public Storage.ReadTransaction delegate() {

                    return delegate;
                }

                @Override
                public CompletableFuture<BatchResponse> read() {

                    return delegate.read().thenApply(result -> {
                        for (final ObjectSchema objectSchema : objectSchemas) {
                            final Map<String, Object> match = result.getVersion(objectSchema, id, version);
                            if (match != null) {
                                return result.with(schema, match);
                            }
                        }
                        return result;
                    });
                }
            };
        }
    }

    interface WriteTransaction extends Storage.WriteTransaction {

        StorageTraits storageTraits(ReferableSchema schema);

        void createObjectLayer(ReferableSchema schema, String id, Map<String, Object> after);

        void updateObjectLayer(ReferableSchema schema, String id, Map<String, Object> before, Map<String, Object> after);

        void deleteObjectLayer(ReferableSchema schema, String id, Map<String, Object> before);

        void writeHistoryLayer(ReferableSchema schema, String id, Map<String, Object> after);

        @Override
        default Storage.WriteTransaction createObject(final ObjectSchema schema, final String id, final Map<String, Object> after) {

            final StorageTraits traits = storageTraits(schema);
            createObjectLayer(schema, id, after);
            schema.getIndirectExtend().forEach(layer -> createObjectLayer(layer, id, after));
            if(hasSyncHistory(traits, schema)) {
                writeHistory(schema, id, after);
            }
            return this;
        }

        @Override
        default Storage.WriteTransaction updateObject(final ObjectSchema schema, final String id, final Map<String, Object> before, final Map<String, Object> after) {

            final StorageTraits traits = storageTraits(schema);
            updateObjectLayer(schema, id, before, after);
            schema.getIndirectExtend().forEach(layer -> updateObjectLayer(layer, id, before, after));
            if(hasSyncHistory(traits, schema)) {
                writeHistory(schema, id, after);
            }
            return this;
        }

        @Override
        default Storage.WriteTransaction deleteObject(final ObjectSchema schema, final String id, final Map<String, Object> before) {

            deleteObjectLayer(schema, id, before);
            schema.getIndirectExtend().forEach(layer -> deleteObjectLayer(layer, id, before));
            return this;
        }

        default Storage.WriteTransaction writeHistory(final ObjectSchema schema, final String id, final Map<String, Object> after) {

            writeHistoryLayer(schema, id, after);
            schema.getIndirectExtend().forEach(layer -> writeHistoryLayer(layer, id, after));
            return this;
        }
    }
}
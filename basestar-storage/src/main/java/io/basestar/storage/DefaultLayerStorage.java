package io.basestar.storage;

import com.google.common.collect.Sets;
import io.basestar.event.Event;
import io.basestar.expression.Expression;
import io.basestar.schema.*;
import io.basestar.storage.exception.UnsupportedQueryException;
import io.basestar.util.Name;
import io.basestar.util.Pager;
import io.basestar.util.Sort;

import java.util.*;
import java.util.concurrent.CompletableFuture;

public interface DefaultLayerStorage extends LayeredStorage, ValidatingStorage {

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

    Pager<Map<String, Object>> queryObject(Consistency consistency, ObjectSchema schema, Expression query, List<Sort> sort, Set<Name> expand);

    @Override
    default Pager<Map<String, Object>> queryView(final Consistency consistency, final ViewSchema schema, final Expression query, final List<Sort> sort, final Set<Name> expand) {

        throw new UnsupportedQueryException(schema.getQualifiedName(), query);
    }

    @Override
    default Pager<Map<String, Object>> queryQuery(final Consistency consistency, final QuerySchema schema, final Map<String, Object> arguments, final Expression query, final List<Sort> sort, final Set<Name> expand) {

        throw new UnsupportedQueryException(schema.getQualifiedName(), query);
    }

    @Override
    default Pager<Map<String, Object>> queryInterface(final Consistency consistency, final InterfaceSchema schema, final Expression query, final List<Sort> sort, final Set<Name> expand) {

        final Collection<ObjectSchema> objectSchemas = schema.getConcreteExtended();
        final Map<String, Pager<Map<String, Object>>> pagers = new HashMap<>();
        objectSchemas.forEach(objectSchema -> {
            pagers.put(objectSchema.getQualifiedName().toString(), queryObject(consistency, objectSchema, query, sort, expand));
        });
        return Pager.merge(Instance.comparator(sort), pagers);
    }

    interface ReadTransaction extends LayeredStorage.ReadTransaction {

        @Override
        default Storage.ReadTransaction get(final ReferableSchema schema, final String id, final Set<Name> expand) {

            if(schema instanceof ObjectSchema) {
                return getObject((ObjectSchema)schema, id, expand);
            } else {
                return getInterface((InterfaceSchema) schema, id, expand);
            }
        }

        @Override
        default Storage.ReadTransaction getVersion(final ReferableSchema schema, final String id, final long version, final Set<Name> expand) {

            if(schema instanceof ObjectSchema) {
                return getObjectVersion((ObjectSchema)schema, id, version, expand);
            } else {
                return getInterfaceVersion((InterfaceSchema) schema, id, version, expand);
            }
        }

        default Storage.ReadTransaction getInterface(final InterfaceSchema schema, final String id, final Set<Name> expand) {

            final Collection<ObjectSchema> objectSchemas = schema.getConcreteExtended();
            objectSchemas.forEach(object -> getObject(object, id, expand));
            final ReadTransaction delegate = this;
            return new Storage.ReadTransaction.Delegating() {

                @Override
                public Storage.ReadTransaction delegate(final ReferableSchema schema) {

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

        default Storage.ReadTransaction getInterfaceVersion(final InterfaceSchema schema, final String id, final long version, final Set<Name> expand) {

            final Collection<ObjectSchema> objectSchemas = schema.getConcreteExtended();
            objectSchemas.forEach(object -> getObjectVersion(object, id, version, expand));
            final ReadTransaction delegate = this;
            return new Storage.ReadTransaction.Delegating() {

                @Override
                public Storage.ReadTransaction delegate(final ReferableSchema schema) {

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

    interface WriteTransaction extends LayeredStorage.WriteTransaction {

        StorageTraits storageTraits(ReferableSchema schema);

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

        @Override
        default Storage.WriteTransaction writeHistory(final ObjectSchema schema, final String id, final Map<String, Object> after) {

            writeHistoryLayer(schema, id, after);
            schema.getIndirectExtend().forEach(layer -> writeHistoryLayer(layer, id, after));
            return this;
        }
    }
}
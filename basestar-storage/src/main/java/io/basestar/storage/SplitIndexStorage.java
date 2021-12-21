package io.basestar.storage;

import io.basestar.event.Event;
import io.basestar.expression.Expression;
import io.basestar.schema.*;
import io.basestar.util.Immutable;
import io.basestar.util.Name;
import io.basestar.util.Pager;
import io.basestar.util.Sort;
import lombok.RequiredArgsConstructor;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

@RequiredArgsConstructor
public class SplitIndexStorage implements IndexStorage {

    private final Storage objectStorage;

    private final IndexStorage indexStorage;

    @Override
    public Pager<Map<String, Object>> query(final Consistency consistency, final QueryableSchema schema, final Map<String, Object> arguments, final Expression query, final List<Sort> sort, final Set<Name> expand) {

        return indexStorage.query(consistency, schema, arguments, query, sort, expand);
    }

    @Override
    public Pager<Map<String, Object>> queryIndex(final ObjectSchema schema, final Index index, final Expression query, final List<Sort> sort, final Set<Name> expand) {

        return indexStorage.queryIndex(schema, index, query, sort, expand);
    }

    @Override
    public CompletableFuture<Set<Event>> afterCreate(final ObjectSchema schema, final String id, final Map<String, Object> after) {

        return objectStorage.afterCreate(schema, id, after)
                .thenCombine(indexStorage.afterCreate(schema, id, after), Immutable::addAll);
    }

    @Override
    public CompletableFuture<Set<Event>> afterUpdate(final ObjectSchema schema, final String id, final long version, final Map<String, Object> before, final Map<String, Object> after) {

        return objectStorage.afterUpdate(schema, id, version, before, after)
                .thenCombine(indexStorage.afterUpdate(schema, id, version, before, after), Immutable::addAll);
    }

    @Override
    public CompletableFuture<Set<Event>> afterDelete(final ObjectSchema schema, final String id, final long version, final Map<String, Object> before) {

        return objectStorage.afterDelete(schema, id, version, before)
                .thenCombine(indexStorage.afterDelete(schema, id, version, before), Immutable::addAll);
    }

    @Override
    public ReadTransaction read(final Consistency consistency) {

        return objectStorage.read(consistency);
    }

    @Override
    public WriteTransaction write(final Consistency consistency, final Versioning versioning) {

        final Storage.WriteTransaction objectTransaction = objectStorage.write(consistency, versioning);
        final IndexStorage.WriteTransaction indexTransaction = indexStorage.write(consistency, versioning);
        return new WriteTransaction() {

            @Override
            public WriteTransaction write(final LinkableSchema schema, final Map<String, Object> after) {

                objectTransaction.write(schema, after);
                return this;
            }

            @Override
            public WriteTransaction createIndexes(final ReferableSchema schema, final List<Index> indexes, final String id, final Map<String, Object> after) {

                indexTransaction.createIndexes(schema, indexes, id, after);
                return this;
            }

            @Override
            public WriteTransaction updateIndexes(final ReferableSchema schema, final List<Index> indexes, final String id, final Map<String, Object> before, final Map<String, Object> after) {

                indexTransaction.updateIndexes(schema, indexes, id, before, after);
                return this;
            }

            @Override
            public WriteTransaction deleteIndexes(final ReferableSchema schema, final List<Index> indexes, final String id, final Map<String, Object> before) {

                indexTransaction.deleteIndexes(schema, indexes, id, before);
                return this;
            }

            @Override
            public Storage.WriteTransaction createObject(final ObjectSchema schema, final String id, final Map<String, Object> after) {

                objectTransaction.createObject(schema, id, after);
                return createIndexes(schema, IndexStorage.getSyncIndexes(indexStorage.storageTraits(schema), schema), id, after);
            }

            @Override
            public Storage.WriteTransaction updateObject(final ObjectSchema schema, final String id, final Map<String, Object> before, final Map<String, Object> after) {

                objectTransaction.updateObject(schema, id, before, after);
                return updateIndexes(schema, IndexStorage.getSyncIndexes(indexStorage.storageTraits(schema), schema), id, before, after);
            }

            @Override
            public Storage.WriteTransaction deleteObject(final ObjectSchema schema, final String id, final Map<String, Object> before) {

                objectTransaction.deleteObject(schema, id, before);
                return deleteIndexes(schema, IndexStorage.getSyncIndexes(indexStorage.storageTraits(schema), schema), id, before);
            }

            @Override
            public Storage.WriteTransaction writeHistory(final ObjectSchema schema, final String id, final Map<String, Object> after) {

                objectTransaction.writeHistory(schema, id, after);
                return this;
            }

            @Override
            public CompletableFuture<BatchResponse> write() {

                return BatchResponse.mergeFutures(Stream.of(objectTransaction.write(), indexTransaction.write()));
            }
        };
    }

    @Override
    public EventStrategy eventStrategy(final ReferableSchema schema) {

        return objectStorage.eventStrategy(schema);
    }

    @Override
    public StorageTraits storageTraits(final ReferableSchema schema) {

        return objectStorage.storageTraits(schema);
    }

    @Override
    public Set<Name> supportedExpand(final LinkableSchema schema, final Set<Name> expand) {

        return objectStorage.supportedExpand(schema, expand);
    }

    @Override
    public void validate(final ObjectSchema schema) {

        objectStorage.validate(schema);
    }
}

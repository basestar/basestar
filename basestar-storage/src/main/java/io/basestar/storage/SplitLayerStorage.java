package io.basestar.storage;

import io.basestar.expression.Expression;
import io.basestar.schema.Consistency;
import io.basestar.schema.LinkableSchema;
import io.basestar.schema.ObjectSchema;
import io.basestar.schema.ReferableSchema;
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
public class SplitLayerStorage implements DefaultLayerStorage {

    private final Storage objectStorage;

    private final LayeredStorage layerStorage;

    @Override
    public Pager<Map<String, Object>> queryObject(final ObjectSchema schema, final Expression query, final List<Sort> sort, final Set<Name> expand) {

        return objectStorage.query(schema, query, sort, expand);
    }

    @Override
    public ReadTransaction read(final Consistency consistency) {

        /*
        The default read implementation in DefaultLayered storage will delegate correctly
         */

        final Storage.ReadTransaction objectTransaction = objectStorage.read(consistency);
        final LayeredStorage.ReadTransaction layerTransaction = layerStorage.read(consistency);
        return new ReadTransaction() {

            @Override
            public Storage.ReadTransaction getObject(final ObjectSchema schema, final String id, final Set<Name> expand) {

                objectTransaction.get(schema, id, expand);
                return this;
            }

            @Override
            public Storage.ReadTransaction getObjectVersion(final ObjectSchema schema, final String id, final long version, final Set<Name> expand) {

                layerTransaction.get(schema, id, expand);
                return this;
            }

            @Override
            public CompletableFuture<BatchResponse> read() {

                return BatchResponse.mergeFutures(Stream.of(objectTransaction.read(), layerTransaction.read()));
            }
        };
    }

    @Override
    public WriteTransaction write(final Consistency consistency, final Versioning versioning) {

        final Storage.WriteTransaction objectTransaction = objectStorage.write(consistency, versioning);
        final LayeredStorage.WriteTransaction layerTransaction = layerStorage.write(consistency, versioning);
        return new WriteTransaction() {

            @Override
            public StorageTraits storageTraits(final ReferableSchema schema) {

                return SplitLayerStorage.this.storageTraits(schema);
            }

            @Override
            public WriteTransaction write(final LinkableSchema schema, final Map<String, Object> after) {

                objectTransaction.write(schema, after);
                return this;
            }

            @Override
            public void createObjectLayer(final ReferableSchema schema, final String id, final Map<String, Object> after) {

                if(schema instanceof ObjectSchema) {
                    objectTransaction.createObject((ObjectSchema)schema, id, after);
                } else {
                    layerTransaction.createObjectLayer(schema, id, after);
                }
            }

            @Override
            public void updateObjectLayer(final ReferableSchema schema, final String id, final Map<String, Object> before, final Map<String, Object> after) {

                if(schema instanceof ObjectSchema) {
                    objectTransaction.updateObject((ObjectSchema)schema, id, before, after);
                } else {
                    layerTransaction.updateObjectLayer(schema, id, before, after);
                }
            }

            @Override
            public void deleteObjectLayer(final ReferableSchema schema, final String id, final Map<String, Object> before) {

                if(schema instanceof ObjectSchema) {
                    objectTransaction.deleteObject((ObjectSchema)schema, id, before);
                } else {
                    layerTransaction.deleteObjectLayer(schema, id, before);
                }
            }

            @Override
            public void writeHistoryLayer(final ReferableSchema schema, final String id, final Map<String, Object> after) {

                layerTransaction.writeHistoryLayer(schema, id, after);
            }

            @Override
            public CompletableFuture<BatchResponse> write() {

                return BatchResponse.mergeFutures(Stream.of(objectTransaction.write(), layerTransaction.write()));
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
}

package io.basestar.database.event;

import io.basestar.schema.Index;
import io.basestar.schema.ObjectSchema;
import lombok.Data;
import lombok.experimental.Accessors;

import java.util.Map;

@Data
@Accessors(chain = true)
public class AsyncIndexUpdatedEvent implements ObjectEvent {

    private String schema;

    private String index;

    private String id;

    private long version;

    private Index.Key key;

    private Map<String, Object> projection;

    public static AsyncIndexUpdatedEvent of(final String schema, final String index, final String id,
                                            final long version, final Index.Key key,
                                            final Map<String, Object> projection) {

        return new AsyncIndexUpdatedEvent().setSchema(schema).setIndex(index).setId(id)
                .setVersion(version).setKey(key).setProjection(projection);
    }

    @Override
    public AsyncIndexUpdatedEvent abbreviate() {

        return new AsyncIndexUpdatedEvent()
                .setSchema(schema)
                .setIndex(index)
                .setId(id)
                .setVersion(version)
                .setKey(key)
                .setProjection(ObjectSchema.readMeta(projection));
    }
}

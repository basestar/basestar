package io.basestar.database.event;

import io.basestar.schema.ObjectSchema;
import lombok.Data;
import lombok.experimental.Accessors;

import java.util.Map;

@Data
@Accessors(chain = true)
public class ObjectDeletedEvent implements ObjectEvent {

    private String schema;

    private String id;

    private long version;

    private Map<String, Object> before;

    public static ObjectDeletedEvent of(final String schema, final String id, final long version,
                                          final Map<String, Object> before) {

        return new ObjectDeletedEvent().setSchema(schema).setId(id).setVersion(version)
                .setBefore(before);
    }

    @Override
    public ObjectDeletedEvent abbreviate() {

        return new ObjectDeletedEvent()
                .setSchema(schema)
                .setId(id)
                .setVersion(version)
                .setBefore(ObjectSchema.readMeta(before));
    }
}

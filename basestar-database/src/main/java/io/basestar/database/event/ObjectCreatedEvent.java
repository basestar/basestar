package io.basestar.database.event;

import io.basestar.schema.ObjectSchema;
import lombok.Data;
import lombok.experimental.Accessors;

import java.util.Map;

@Data
@Accessors(chain = true)
public class ObjectCreatedEvent implements ObjectEvent {

    private String schema;

    private String id;

    private Map<String, Object> after;

    public static ObjectCreatedEvent of(final String schema, final String id, final Map<String, Object> after) {

        return new ObjectCreatedEvent().setSchema(schema).setId(id).setAfter(after);
    }

    @Override
    public ObjectCreatedEvent abbreviate() {

        return new ObjectCreatedEvent()
                .setSchema(schema)
                .setId(id)
                .setAfter(ObjectSchema.readMeta(after));
    }
}

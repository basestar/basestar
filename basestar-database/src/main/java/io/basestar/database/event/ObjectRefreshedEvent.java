package io.basestar.database.event;

import io.basestar.schema.ObjectSchema;
import lombok.Data;
import lombok.experimental.Accessors;

import java.util.Map;

@Data
@Accessors(chain = true)
public class ObjectRefreshedEvent implements ObjectEvent {

    private String schema;

    private String id;

    private long version;

    private Map<String, Object> before;

    private Map<String, Object> after;

    public static ObjectRefreshedEvent of(final String schema, final String id, final long version,
                                          final Map<String, Object> before, final Map<String, Object> after) {

        return new ObjectRefreshedEvent().setSchema(schema).setId(id).setVersion(version)
                .setBefore(before).setAfter(after);
    }

    @Override
    public ObjectRefreshedEvent abbreviate() {

        return new ObjectRefreshedEvent()
                .setSchema(schema)
                .setId(id)
                .setVersion(version)
                .setBefore(ObjectSchema.readMeta(before))
                .setBefore(ObjectSchema.readMeta(after));
    }
}

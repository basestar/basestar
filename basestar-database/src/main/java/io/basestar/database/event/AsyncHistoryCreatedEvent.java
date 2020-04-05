package io.basestar.database.event;

import io.basestar.schema.ObjectSchema;
import lombok.Data;
import lombok.experimental.Accessors;

import java.util.Map;

@Data
@Accessors(chain = true)
public class AsyncHistoryCreatedEvent implements ObjectEvent {

    private String schema;

    private String id;

    private long version;

    private Map<String, Object> after;

    public static AsyncHistoryCreatedEvent of(final String schema, final String id, final long version, final Map<String, Object> after) {

        return new AsyncHistoryCreatedEvent().setSchema(schema).setId(id).setVersion(version).setAfter(after);
    }

    @Override
    public AsyncHistoryCreatedEvent abbreviate() {

        return new AsyncHistoryCreatedEvent()
                .setSchema(schema)
                .setId(id)
                .setVersion(version)
                .setAfter(ObjectSchema.readMeta(after));
    }
}

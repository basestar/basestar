package io.basestar.database.event;

import io.basestar.event.Event;
import io.basestar.schema.Ref;
import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class RefRefreshEvent implements RefEvent, ObjectEvent {

    private Ref ref;

    private String schema;

    private String id;

    public static RefRefreshEvent of(final Ref ref, final String schema, final String id) {

        return new RefRefreshEvent().setRef(ref).setSchema(schema).setId(id);
    }

    @Override
    public Event abbreviate() {

        return this;
    }
}

package io.basestar.stream;

import lombok.Data;
import lombok.experimental.Accessors;

import java.util.Map;

@Data
@Accessors(chain = true)
public class Change {

    private Event event;

    private String schema;

    private String id;

    private Map<String, Object> before;

    private Map<String, Object> after;

    public static Change of(final Event event, final String schema, final String id, final Map<String, Object> before, final Map<String, Object> after) {

        return new Change().setEvent(event).setSchema(schema).setId(id).setBefore(before).setAfter(after);
    }

    public enum Event {

        CREATE,
        UPDATE,
        DELETE
    }
}

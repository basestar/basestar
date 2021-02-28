package io.basestar.storage.replica.event;

import io.basestar.event.Event;
import io.basestar.schema.Consistency;
import io.basestar.storage.Versioning;
import io.basestar.util.Name;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.With;
import lombok.experimental.Accessors;

import java.util.Map;

@Data
@With
@NoArgsConstructor
@AllArgsConstructor
@Accessors(chain = true)
public class ReplicaSyncEvent implements Event {

    public enum Action {

        CREATE,
        UPDATE,
        DELETE,
//        HISTORY,
//        INDEX_CREATE,
//        INDEX_UPDATE,
//        INDEX_DELETE,
        WRITE
    }

    private Action action;
    
    private Name schema;

    private String id;

    private Map<String, Object> before;

    private Map<String, Object> after;

    private Consistency consistency;

    private Versioning versioning;

    public static ReplicaSyncEvent create(final Name schema, final String id, final Map<String, Object> after, final Consistency consistency, final Versioning versioning) {

        return new ReplicaSyncEvent(Action.CREATE, schema, id, null, after, consistency, versioning);
    }

    public static ReplicaSyncEvent update(final Name schema, final String id, final Map<String, Object> before, final Map<String, Object> after, final Consistency consistency, final Versioning versioning) {

        return new ReplicaSyncEvent(Action.UPDATE, schema, id, before, after, consistency, versioning);
    }

    public static ReplicaSyncEvent delete(final Name schema, final String id, final Map<String, Object> before, final Consistency consistency, final Versioning versioning) {

        return new ReplicaSyncEvent(Action.DELETE, schema, id, before, null, consistency, versioning);
    }

    public static ReplicaSyncEvent write(final Name schema, final Map<String, Object> after, final Consistency consistency, final Versioning versioning) {

        return new ReplicaSyncEvent(Action.WRITE, schema, null, null, after, consistency, versioning);
    }

    @Override
    public Event abbreviate() {
        
        return new ReplicaSyncEvent(action, schema, id, null, null, consistency, versioning);
    }
}

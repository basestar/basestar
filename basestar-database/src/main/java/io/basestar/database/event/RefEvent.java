package io.basestar.database.event;

import io.basestar.event.Event;
import io.basestar.schema.Ref;

public interface RefEvent extends Event {

    Ref getRef();

    String getSchema();
}

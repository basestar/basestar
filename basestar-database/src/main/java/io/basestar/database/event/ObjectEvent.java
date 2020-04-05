package io.basestar.database.event;

import io.basestar.event.Event;

public interface ObjectEvent extends Event {

    String getSchema();

    String getId();
}

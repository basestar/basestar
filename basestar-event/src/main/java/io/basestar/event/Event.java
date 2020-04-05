package io.basestar.event;

//@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = Event.EVENT)
public interface Event {

    String EVENT = "@event";

//    String getUniqueId();

    Event abbreviate();
}

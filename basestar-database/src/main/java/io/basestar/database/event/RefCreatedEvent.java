//package io.basestar.database.event;
//
//import io.basestar.event.Event;
//import io.basestar.schema.Reserved;
//import io.basestar.util.Path;
//import lombok.Data;
//import lombok.experimental.Accessors;
//
//import java.util.Map;
//import java.util.Set;
//
//@Data
//@Accessors(chain = true)
//public class RefCreatedEvent implements Event {
////
////    public static final String EVENT = "RefCreated";
//
//    private String schema;
//
//    private String id;
//
//    private Map<String, Object> after;
//
//    private Set<Path> paths;
//
////    private boolean abbreviated;
//
////    @Override
////    public String getEvent() {
////
////        return EVENT;
////    }
////
////    @Override
////    public Event abbreviated() {
////
////        return new RefCreatedEvent()
////                .setSchema(schema).setId(id)
////                .setAbbreviated(true)
////                .setAfter(ObjectEvent.abbreviate(after))
////                .setPaths(paths);
////    }
////
////    @Override
////    public String group() {
////
////        return getSchema() + Reserved.DELIMITER + getId();
////    }
//}

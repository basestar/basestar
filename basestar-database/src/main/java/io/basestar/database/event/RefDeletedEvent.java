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
//public class RefDeletedEvent implements Event {
//
////    public static final String EVENT = "RefDeleted";
//
//    private String schema;
//
//    private String id;
//
//    private Map<String, Object> before;
//
//    private Set<Path> paths;
//
////    private boolean abbreviated;
////
////    @Override
////    public String getEvent() {
////
////        return EVENT;
////    }
////
////    @Override
////    public Event abbreviated() {
////
////        return new RefDeletedEvent()
////                .setSchema(schema).setId(id)
////                .setAbbreviated(true)
////                .setBefore(ObjectEvent.abbreviate(before))
////                .setPaths(paths);
////    }
////
////    @Override
////    public String group() {
////
////        return getSchema() + Reserved.DELIMITER + getId();
////    }
//}

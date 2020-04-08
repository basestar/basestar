//package io.basestar.database.event;

/*-
 * #%L
 * basestar-database
 * %%
 * Copyright (C) 2019 - 2020 Basestar.IO
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
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

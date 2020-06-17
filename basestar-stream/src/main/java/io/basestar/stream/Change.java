package io.basestar.stream;

/*-
 * #%L
 * basestar-stream
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

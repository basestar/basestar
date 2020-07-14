package io.basestar.database.event;

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

import io.basestar.schema.ObjectSchema;
import io.basestar.util.Name;
import lombok.Data;
import lombok.experimental.Accessors;

import java.util.Map;

@Data
@Accessors(chain = true)
public class ObjectDeletedEvent implements ObjectEvent {

    private Name schema;

    private String id;

    private long version;

    private Map<String, Object> before;

    public static ObjectDeletedEvent of(final Name schema, final String id, final long version,
                                        final Map<String, Object> before) {

        return new ObjectDeletedEvent().setSchema(schema).setId(id).setVersion(version)
                .setBefore(before);
    }

    @Override
    public ObjectDeletedEvent abbreviate() {

        return new ObjectDeletedEvent()
                .setSchema(schema)
                .setId(id)
                .setVersion(version)
                .setBefore(ObjectSchema.readMeta(before));
    }
}

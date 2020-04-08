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

import io.basestar.schema.Index;
import io.basestar.schema.ObjectSchema;
import lombok.Data;
import lombok.experimental.Accessors;

import java.util.Map;

@Data
@Accessors(chain = true)
public class AsyncIndexUpdatedEvent implements ObjectEvent {

    private String schema;

    private String index;

    private String id;

    private long version;

    private Index.Key key;

    private Map<String, Object> projection;

    public static AsyncIndexUpdatedEvent of(final String schema, final String index, final String id,
                                            final long version, final Index.Key key,
                                            final Map<String, Object> projection) {

        return new AsyncIndexUpdatedEvent().setSchema(schema).setIndex(index).setId(id)
                .setVersion(version).setKey(key).setProjection(projection);
    }

    @Override
    public AsyncIndexUpdatedEvent abbreviate() {

        return new AsyncIndexUpdatedEvent()
                .setSchema(schema)
                .setIndex(index)
                .setId(id)
                .setVersion(version)
                .setKey(key)
                .setProjection(ObjectSchema.readMeta(projection));
    }
}

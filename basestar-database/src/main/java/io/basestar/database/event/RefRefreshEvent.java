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

import io.basestar.event.Event;
import io.basestar.schema.util.Ref;
import io.basestar.util.Name;
import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class RefRefreshEvent implements RefEvent, ObjectEvent {

    private Ref ref;

    private Name schema;

    private String id;

    public static RefRefreshEvent of(final Ref ref, final Name schema, final String id) {

        return new RefRefreshEvent().setRef(ref).setSchema(schema).setId(id);
    }

    @Override
    public Event abbreviate() {

        return this;
    }
}

package io.basestar.database.util;

/*-
 * #%L
 * basestar-database-server
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

import io.basestar.schema.Instance;
import io.basestar.schema.ObjectSchema;
import io.basestar.util.Name;
import lombok.Data;
import lombok.RequiredArgsConstructor;

import java.util.Map;

@Data
@RequiredArgsConstructor
public class RefKey {

    private final Name schema;

    private final String id;

    public static RefKey from(final Map<String, Object> item) {

        final Name schema = Instance.getSchema(item);
        final String id = Instance.getId(item);
        assert schema != null && id != null;
        return new RefKey(schema, id);
    }

    public static RefKey from(final ObjectSchema schema, final Map<String, Object> item) {

        final String id = Instance.getId(item);
        assert id != null;
        return new RefKey(schema.getQualifiedName(), id);
    }
}

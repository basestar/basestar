package io.basestar.schema.migration;

/*-
 * #%L
 * basestar-schema
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

import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.schema.ObjectSchema;
import io.basestar.schema.Property;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class SchemaMigration implements Serializable {

    private String source;

    private Map<String, Expression> properties;

    public Map<String, Object> migrate(final ObjectSchema sourceSchema, final ObjectSchema targetSchema, final Map<String, Object> source) {

        final Context context = Context.init(source);
        final Map<String, Object> result = new HashMap<>();
        for(final Map.Entry<String, Property> targetEntry : targetSchema.getAllProperties().entrySet()) {
            final String name = targetEntry.getKey();
            //final Property targetProperty = targetEntry.getValue();
            final Property sourceProperty = sourceSchema.getProperty(name, true);
            final Expression migration = properties.get(name);
            if(migration != null) {
                final Object value = migration.evaluate(context);
                result.put(name, value);
            } else if(sourceProperty != null) {
                final Object value = source.get(name);
                result.put(name, value);
            }
        }
        return targetSchema.create(result);
    }
}

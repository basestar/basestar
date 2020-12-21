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
import io.basestar.schema.LinkableSchema;
import io.basestar.schema.Reserved;
import io.basestar.schema.use.Use;
import io.basestar.util.Name;
import lombok.Data;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@Data
public class Migration implements Serializable {

    private Map<String, Expression> properties;

    private Expression operation;

    public Map<String, Object> migrate(final LinkableSchema schema, final Map<String, Object> source) {

        final Map<String, Object> result = new HashMap<>();
        schema.metadataSchema().forEach((k, v) -> {
            result.put(k, property(k, v, null, source));
        });
        final Map<String, Set<Name>> branches = Name.branch(schema.getExpand());
        schema.getProperties().forEach((k, v) -> {
            result.put(k, property(k, v.typeOf(), branches.get(k), source));
        });
        final String operation = operation(source);
        if("delete".equalsIgnoreCase(operation)) {
            result.put(Reserved.DELETED, true);
        }
        return result;
    }

    private String operation(final Map<String, Object> source) {

        if(operation == null) {
            return "upsert";
        } else {
            return operation.evaluateAs(String.class, Context.init(source));
        }
    }

    private Object property(final String key, final Use<?> type, final Set<Name> expand, final Map<String, Object> source) {

        final Map<String, Expression> properties = getProperties();
        final Expression expr = properties.get(key);
        final Object value;
        if(expr != null) {
            value = expr.evaluate(Context.init(source));
        } else {
            value = source.get(key);
        }
        return type.create(value, expand,true);
    }
}

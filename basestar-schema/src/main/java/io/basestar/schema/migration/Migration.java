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
import io.basestar.schema.Loadable;
import io.basestar.schema.use.Use;
import io.basestar.util.Immutable;
import io.basestar.util.Name;
import lombok.Data;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@Data
public class Migration implements Loadable {

    private final Map<String, Expression> properties;

    private final Expression operation;

    @Data
    public static class Builder implements Loadable.Descriptor {

        private Map<String, Expression> properties;

        private Expression operation;

        @Override
        public Object jsonValue() {

            return this;
        }

        public Migration build() {

            return new Migration(properties, operation);
        }

        public static Builder load(final URL url) throws IOException {

            return YAML_MAPPER.readValue(url, Builder.class);
        }

        public static Builder load(final InputStream is) throws IOException {

            return YAML_MAPPER.readValue(is, Builder.class);
        }
    }

    public Migration(final Map<String, Expression> properties, final Expression operation) {

        this.properties = Immutable.copy(properties);
        this.operation = operation;
    }

    public Map<String, Object> migrate(final LinkableSchema schema, final Map<String, Object> source) {

        final Map<String, Object> result = new HashMap<>();
        schema.metadataSchema().forEach((k, v) -> {
            result.put(k, property(k, v, null, source));
        });
        final Map<String, Set<Name>> branches = Name.branch(schema.getExpand());
        schema.getProperties().forEach((k, v) -> {
            result.put(k, property(k, v.typeOf(), branches.get(k), source));
        });
        return result;
    }

    public Operation operation(final Map<String, Object> source) {

        if(operation == null) {
            return Operation.UPSERT;
        } else {
            return Operation.parse(operation.evaluateAs(String.class, Context.init(source)));
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

    public static Migration load(final URL url) throws IOException {

        return Builder.load(url).build();
    }

    public static Migration load(final InputStream is) throws IOException {

        return Builder.load(is).build();
    }
}

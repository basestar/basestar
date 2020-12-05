package io.basestar.mapper.internal;

/*-
 * #%L
 * basestar-mapper
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

import io.basestar.expression.type.Coercion;
import io.basestar.mapper.MappingContext;
import io.basestar.schema.Instance;
import io.basestar.schema.ObjectSchema;
import io.basestar.type.PropertyContext;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;

public class MetadataMapper implements MemberMapper<ObjectSchema.Builder> {

    private final Name name;

    private final PropertyContext property;

    private final TypeMapper type;

    public MetadataMapper(final MappingContext context, final Name name, final PropertyContext property) {

        this.name = name;
        this.property = property;
        this.type = context.typeMapper(property);
    }

    @Override
    public TypeMapper getType() {

        return type;
    }

    @Override
    public String memberType() {

        return name.name().toLowerCase();
    }

    @Override
    public void addToSchema(final InstanceSchemaMapper<ObjectSchema.Builder, ?> mapper, final ObjectSchema.Builder builder) {

        // no-op
    }

    @Override
    public void unmarshall(final Object source, final Map<String, Object> target) throws InvocationTargetException, IllegalAccessException {

        if(property.canGet()) {
            final Object value = property.get(source);
            name.unmarshall(type, target, value);
        }
    }

    @Override
    public void marshall(final Map<String, Object> source, final Object target) throws InvocationTargetException, IllegalAccessException {

        if(property.canSet()) {
            final Object value = name.marshall(type, source);
            property.set(target, value);
        }
    }

    public enum Name {

        CREATED {
            @Override
            public Object marshall(final TypeMapper type, final Map<String, Object> source) {

                return type.marshall(Instance.getCreated(source));
            }

            @Override
            public void unmarshall(final TypeMapper type, final Map<String, Object> target, final Object value) {

                Instance.setCreated(target, Coercion.toDateTime(type.unmarshall(value)));
            }
        },
        UPDATED {
            @Override
            public Object marshall(final TypeMapper type, final Map<String, Object> source) {

                return type.marshall(Instance.getUpdated(source));
            }

            @Override
            public void unmarshall(final TypeMapper type, final Map<String, Object> target, final Object value) {

                Instance.setUpdated(target, Coercion.toDateTime(type.unmarshall(value)));
            }
        },
        HASH {
            @Override
            public Object marshall(final TypeMapper type, final Map<String, Object> source) {

                return type.marshall(Instance.getHash(source));
            }

            @Override
            public void unmarshall(final TypeMapper type, final Map<String, Object> target, final Object value) {

                Instance.setHash(target, Coercion.toString(type.unmarshall(value)));
            }
        },
        VERSION {
            @Override
            public Object marshall(final TypeMapper type, final Map<String, Object> source) {

                return type.marshall(Instance.getVersion(source));
            }

            @Override
            public void unmarshall(final TypeMapper type, final Map<String, Object> target, final Object value) {

                Instance.setVersion(target, Coercion.toLong(type.unmarshall(value)));
            }
        };

        public abstract Object marshall(TypeMapper type, Map<String, Object> source);

        public abstract void unmarshall(TypeMapper type, Map<String, Object> target, Object value);
    }
}

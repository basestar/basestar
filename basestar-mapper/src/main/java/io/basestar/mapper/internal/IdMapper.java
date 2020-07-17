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

import io.basestar.expression.Expression;
import io.basestar.expression.type.Coercion;
import io.basestar.mapper.MappingContext;
import io.basestar.schema.Id;
import io.basestar.schema.Instance;
import io.basestar.schema.ObjectSchema;
import io.basestar.type.PropertyContext;

import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class IdMapper implements MemberMapper<ObjectSchema.Builder> {

    private final PropertyContext property;

    private final TypeMapper type;

    private final Expression expression;

    public IdMapper(final MappingContext context, final PropertyContext property) {

        this.property = property;
        this.type = TypeMapper.from(context, property.type());
        this.expression = null;
    }

    public IdMapper(final IdMapper copy, final Expression expression) {

        this.property = copy.property;
        this.type = copy.type;
        this.expression = expression;
    }

    @Override
    public IdMapper withExpression(final Expression expression) {

        return new IdMapper(this, expression);
    }

    @Override
    public TypeMapper getType() {

        return type;
    }

    @Override
    public void addToSchema(final InstanceSchemaMapper<?, ObjectSchema.Builder> mapper, final ObjectSchema.Builder builder) {

        if(expression != null) {
            builder.setId(Id.builder().setExpression(expression));
        }
    }

    @Override
    public void unmarshall(final Object source, final Map<String, Object> target) throws InvocationTargetException, IllegalAccessException {

        if(property.canGet()) {
            final String id = Coercion.toString(type.unmarshall(property.get(source)));
            Instance.setId(target, id);
        }
    }

    @Override
    public void marshall(final Map<String, Object> source, final Object target) throws InvocationTargetException, IllegalAccessException {

        if(property.canSet()) {
            final String id = Instance.getId(source);
            property.set(target, type.marshall(id));
        }
    }

    @Override
    public Set<Class<?>> dependencies() {

        return Collections.emptySet();
    }
}

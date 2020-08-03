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
import io.basestar.mapper.MappingContext;
import io.basestar.schema.Constraint;
import io.basestar.schema.InstanceSchema;
import io.basestar.schema.Property;
import io.basestar.type.AnnotationContext;
import io.basestar.type.PropertyContext;
import io.basestar.type.SerializableAccessor;
import io.basestar.type.has.HasType;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class PropertyMapper implements MemberMapper<InstanceSchema.Builder> {

    private final String name;

    private final SerializableAccessor property;

    private final TypeMapper type;

    private final List<Constraint> constraints;

    private final String description;

    private final Expression expression;

    private final boolean immutable;

    public PropertyMapper(final MappingContext context, final String name, final PropertyContext property) {

        this.name = name;
        this.property = property.serializableAccessor();
        this.type = context.typeMapper(property);
        this.constraints = constraints(property);
        this.description = null;
        this.expression = null;
        this.immutable = false;
    }

    private PropertyMapper(final PropertyMapper copy, final String description, final Expression expression, final boolean immutable) {

        this.name = copy.name;
        this.property = copy.property;
        this.type = copy.type;
        this.constraints = copy.constraints;
        this.description = description;
        this.expression = expression;
        this.immutable = immutable;
    }

    @Override
    public PropertyMapper withExpression(final Expression expression) {

        return new PropertyMapper(this, description, expression, immutable);
    }

    @Override
    public PropertyMapper withDescription(final String description) {

        return new PropertyMapper(this, description, expression, immutable);
    }

    public PropertyMapper withImmutable(final boolean immutable) {

        return new PropertyMapper(this, description, expression, immutable);
    }

    @Override
    public TypeMapper getType() {

        return type;
    }

    @Override
    public String memberType() {

        return "property";
    }

    @Override
    public void addToSchema(final InstanceSchemaMapper<?, InstanceSchema.Builder> mapper, final InstanceSchema.Builder builder) {

        mapper.addProperty(builder, name, Property.builder()
                .setExpression(expression)
                .setImmutable(immutable ? true : null)
                .setDescription(description)
                .setType(this.type.use())
                .setConstraints(constraints.isEmpty() ? null : constraints));
    }

    @Override
    public void unmarshall(final Object source, final Map<String, Object> target) throws InvocationTargetException, IllegalAccessException {

        if(property.canGet()) {
            final Object value = property.get(source);
            target.put(name, type.unmarshall(value));
        }
    }

    @Override
    public void marshall(final Map<String, Object> source, final Object target) throws InvocationTargetException, IllegalAccessException {

        if(property.canSet()) {
            final Object value = source.get(name);
            property.set(target, type.marshall(value));
        }
    }

    private static List<Constraint> constraints(final PropertyContext property) {

        final List<AnnotationContext<?>> constraintAnnotations = property.allAnnotations().stream()
                .filter(a -> a.type().annotations().stream()
                        .anyMatch(HasType.match(javax.validation.Constraint.class)))
                .collect(Collectors.toList());

        final List<Constraint> constraints = new ArrayList<>();
        constraintAnnotations.forEach(annot -> {
            final String message = annot.<String>nonDefaultValue("message").orElse(null);
            Constraint.fromJsr380(null, annot.annotation(), message).ifPresent(constraints::add);
        });
        return constraints;
    }
}

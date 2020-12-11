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
import io.basestar.mapper.exception.MappingException;
import io.basestar.schema.Link;
import io.basestar.schema.LinkableSchema;
import io.basestar.schema.exception.UnexpectedTypeException;
import io.basestar.type.PropertyContext;
import io.basestar.type.SerializableAccessor;
import io.basestar.util.Sort;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;

public class LinkMapper<B extends LinkableSchema.Builder<B, ?>> implements MemberMapper<B> {

    private final String name;

    private final SerializableAccessor property;

    private final TypeMapper type;

    private final TypeMapper.OfCustom itemType;

    private final boolean single;

    private final Expression expression;

    private final List<Sort> sort;

    private final String description;

    public LinkMapper(final MappingContext context, final String name, final PropertyContext property, final Expression expression, final List<Sort> sort) {

        this.name = name;
        this.property = property.serializableAccessor();
        this.type = context.typeMapper(property);
        final TypeMapper requiredType;
        if(type instanceof TypeMapper.OfOptional) {
            requiredType = ((TypeMapper.OfOptional) type).getValue();
        } else {
            requiredType = type;
        }
        if(requiredType instanceof TypeMapper.OfArray) {
            final TypeMapper.OfArray array = (TypeMapper.OfArray)requiredType;
            if(array.getValue() instanceof TypeMapper.OfCustom) {
                itemType = (TypeMapper.OfCustom)array.getValue();
                single = false;
            } else {
                throw new IllegalStateException("Cannot create link item mapper for " + array.getValue());
            }
        } else if(requiredType instanceof TypeMapper.OfCustom) {
            itemType = (TypeMapper.OfCustom) requiredType;
            single = true;
        } else {
            throw new IllegalStateException("Cannot create link mapper for " + requiredType);
        }
        this.expression = expression;
        this.sort = sort;
        this.description = null;
    }

    public LinkMapper(final LinkMapper<B> copy, final String description) {

        this.name = copy.name;
        this.property = copy.property;
        this.type = copy.type;
        this.itemType = copy.itemType;
        this.single = copy.single;
        this.expression = copy.expression;
        this.sort = copy.sort;
        this.description = description;
    }

    @Override
    public LinkMapper<B> withDescription(final String description) {

        return new LinkMapper<>(this, description);
    }

    @Override
    public TypeMapper getType() {

        return type;
    }

    @Override
    public String memberType() {

        return "link";
    }

    @Override
    public void addToSchema(final InstanceSchemaMapper<B, ?> mapper, final B builder) {

        mapper.addLink(builder, name, Link.builder()
                .setExpression(expression)
                .setSingle(single ? true : null)
                .setSchema(itemType.getQualifiedName())
                .setSort(sort == null ? null : (sort.isEmpty() ? null : sort))
                .setDescription(description));
    }

    @Override
    public void unmarshall(final Object source, final Map<String, Object> target) throws InvocationTargetException, IllegalAccessException {

        try {
            if (property.canGet()) {
                final Object value = property.get(source);
                target.put(name, type.unmarshall(value));
            }
        } catch (final UnexpectedTypeException e) {
            throw new MappingException(this.name, e);
        }
    }

    @Override
    public void marshall(final Map<String, Object> source, final Object target) throws InvocationTargetException, IllegalAccessException {

        if(property.canSet()) {
            final Object value = source.get(name);
            property.set(target, type.marshall(value));
        }
    }
}

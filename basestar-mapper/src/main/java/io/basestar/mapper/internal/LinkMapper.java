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
import io.basestar.schema.InstanceSchema;
import io.basestar.schema.Link;
import io.basestar.type.PropertyContext;
import io.basestar.util.Sort;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;

public class LinkMapper implements MemberMapper<InstanceSchema.Builder> {

    private final String name;

    private final PropertyContext property;

    private final TypeMapper type;

    private final TypeMapper.OfCustom itemType;

    private final boolean single;

    private final Expression expression;

    private final List<Sort> sort;

    private final String description;

    public LinkMapper(final MappingContext context, final String name, final PropertyContext property, final Expression expression, final List<Sort> sort) {

        this.name = name;
        this.property = property;
        this.type = TypeMapper.from(context, property.type());
        if(type instanceof TypeMapper.OfArray) {
            final TypeMapper.OfArray array = (TypeMapper.OfArray)type;
            if(array.getValue() instanceof TypeMapper.OfCustom) {
                itemType = (TypeMapper.OfCustom)array.getValue();
                single = false;
            } else {
                throw new IllegalStateException("Cannot create link item mapper for " + array.getValue());
            }
        } else if(type instanceof TypeMapper.OfCustom) {
            itemType = (TypeMapper.OfCustom) type;
            single = true;
        } else {
            throw new IllegalStateException("Cannot create link mapper for " + type);
        }
        this.expression = expression;
        this.sort = sort;
        this.description = null;
    }

    public LinkMapper(final LinkMapper copy, final String description) {

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
    public LinkMapper withDescription(final String description) {

        return new LinkMapper(this, description);
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
    public void addToSchema(final InstanceSchemaMapper<?, InstanceSchema.Builder> mapper, final InstanceSchema.Builder builder) {

        mapper.addLink(builder, name, Link.builder()
                .setExpression(expression)
                .setSingle(single ? true : null)
                .setSchema(itemType.getQualifiedName())
                .setSort(sort == null ? null : (sort.isEmpty() ? null : sort))
                .setDescription(description));
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
}

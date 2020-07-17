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

import com.google.common.collect.ImmutableList;
import io.basestar.expression.Expression;
import io.basestar.mapper.MappingContext;
import io.basestar.schema.Property;
import io.basestar.schema.ViewSchema;
import io.basestar.type.TypeContext;
import io.basestar.util.Name;

import java.util.List;
import java.util.Set;

public class ViewSchemaMapper<T> extends InstanceSchemaMapper<T, ViewSchema.Builder> {

    private final Name fromSchema;

    private final Set<Name> fromExpand;

    private final boolean materialized;

    private final List<String> group;

    private final Expression where;

    public ViewSchemaMapper(final MappingContext context, final Name name, final TypeContext type, final Name fromSchema, final Set<Name> fromExpand, final boolean materialized) {

        super(context, name, type, ViewSchema.Builder.class);
        this.fromSchema = fromSchema;
        this.fromExpand = fromExpand;
        this.materialized = materialized;
        this.group = ImmutableList.of();
        this.where = null;
    }

    private ViewSchemaMapper(final ViewSchemaMapper<T> copy, final List<String> group, final Expression where) {

        super(copy);
        this.fromSchema = copy.fromSchema;
        this.fromExpand = copy.fromExpand;
        this.materialized = copy.materialized;
        this.group = group;
        this.where = where;
    }

    public ViewSchemaMapper<T> withWhere(final Expression where) {

        return new ViewSchemaMapper<>(this, group, where);
    }

    public ViewSchemaMapper<T> withGroup(final List<String> group) {

        return new ViewSchemaMapper<>(this, group, where);
    }

    @Override
    public ViewSchema.Builder schema() {

        final ViewSchema.From.Builder from = ViewSchema.From.builder()
                .setSchema(fromSchema)
                .setExpand(fromExpand.isEmpty() ? null : fromExpand);

        return addMembers(ViewSchema.builder()
                .setFrom(from)
                .setWhere(where));
    }

    protected void addProperty(final ViewSchema.Builder builder, final String name, final Property.Builder property) {

        if(group.contains(name)) {
            builder.setGroup(name, property);
        } else {
            builder.setSelect(name, property);
        }
    }
}
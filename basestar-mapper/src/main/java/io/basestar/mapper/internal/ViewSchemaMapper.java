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
import com.google.common.collect.ImmutableSet;
import io.basestar.expression.Expression;
import io.basestar.mapper.MappingContext;
import io.basestar.schema.ViewSchema;
import io.basestar.type.TypeContext;
import io.basestar.util.Name;

import java.util.List;
import java.util.Set;

public class ViewSchemaMapper<T> extends InstanceSchemaMapper<T, ViewSchema.Builder> {

    private final boolean materialized;

    private final Name fromSchema;

    private final Set<Name> fromExpand;

    private final List<String> group;

    private final Expression where;

    public ViewSchemaMapper(final MappingContext context, final Name name, final TypeContext type, final boolean materialized) {

        super(ViewSchema.Builder.class, context, name, type);
        this.materialized = materialized;
        this.fromSchema = null;
        this.fromExpand = ImmutableSet.of();
        this.group = ImmutableList.of();
        this.where = null;
    }

    private ViewSchemaMapper(final ViewSchemaMapper<T> copy, final String description, final Name fromSchema,
                             final Set<Name> fromExpand, final List<String> group, final Expression where) {

        super(copy, description);
        this.materialized = copy.materialized;
        this.fromSchema = fromSchema;
        this.fromExpand = fromExpand;
        this.group = group;
        this.where = where;
    }

    public ViewSchemaMapper<T> withFrom(final Name fromSchema, final Set<Name> fromExpand) {

        return new ViewSchemaMapper<>(this, description, fromSchema, fromExpand, group, where);
    }

    public ViewSchemaMapper<T> withWhere(final Expression where) {

        return new ViewSchemaMapper<>(this, description, fromSchema, fromExpand, group, where);
    }

    public ViewSchemaMapper<T> withGroup(final List<String> group) {

        return new ViewSchemaMapper<>(this, description, fromSchema, fromExpand, group, where);
    }

    @Override
    public ViewSchemaMapper<T> withDescription(final String description) {

        return new ViewSchemaMapper<>(this, description, fromSchema, fromExpand, group, where);
    }

    @Override
    public ViewSchema.Builder schemaBuilder() {

        final ViewSchema.From.Builder from = ViewSchema.From.builder()
                .setSchema(fromSchema)
                .setExpand(fromExpand.isEmpty() ? null : fromExpand);

        return addMembers(ViewSchema.builder()
                .setFrom(from)
                .setGroup(group)
                .setWhere(where));
    }
}
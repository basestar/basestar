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
import io.basestar.schema.Bucketing;
import io.basestar.schema.Permission;
import io.basestar.schema.ViewSchema;
import io.basestar.type.TypeContext;
import io.basestar.util.Immutable;
import io.basestar.util.Name;
import lombok.Data;
import lombok.experimental.Accessors;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class ViewSchemaMapper<T> extends InstanceSchemaMapper<ViewSchema.Builder, T> {

    private final boolean materialized;

    private final Name fromSchema;

    private final Set<Name> fromExpand;

    private final List<String> group;

    private final Expression where;

    public ViewSchemaMapper(final Builder<T> builder) {

        super(ViewSchema.Builder.class, builder);
        this.materialized = builder.materialized;
        this.fromSchema = builder.fromSchema;
        this.fromExpand = Immutable.set(builder.fromExpand);
        this.group = Immutable.list(builder.group);
        this.where = builder.where;
    }

    public static <T> Builder<T> builder(final MappingContext context, final Name name, final TypeContext type) {

        return new Builder<>(context, name, type);
    }

    @Override
    public ViewSchema.Builder schemaBuilder() {

        final ViewSchema.From.Builder from = ViewSchema.From.builder()
                .setSchema(fromSchema)
                .setExpand(fromExpand.isEmpty() ? null : fromExpand);

        return addMembers(ViewSchema.builder()
                .setMaterialized(materialized)
                .setFrom(from)
                .setGroup(group)
                .setWhere(where));
    }

    @Data
    @Accessors(chain = true)
    public static class Builder<T> implements InstanceSchemaMapper.Builder<ViewSchema.Builder, T> {

        private final MappingContext context;

        private final Name name;

        private final TypeContext type;

        private String description;

        private List<Bucketing> bucketing;

        private boolean materialized;

        private Name fromSchema;

        private Set<Name> fromExpand;

        private List<String> group;

        private Expression where;

        private Map<String, Permission.Descriptor> permissions;

        @Override
        public ViewSchemaMapper<T> build() {

            return new ViewSchemaMapper<>(this);
        }
    }
}
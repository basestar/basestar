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

import io.basestar.mapper.MappingContext;
import io.basestar.schema.*;
import io.basestar.type.TypeContext;
import io.basestar.util.Immutable;
import io.basestar.util.Name;
import lombok.Data;
import lombok.experimental.Accessors;

import java.util.List;
import java.util.Map;

public class ObjectSchemaMapper<T> extends LinkableSchemaMapper<ObjectSchema.Builder, T> {

    private final Map<String, Index.Descriptor> indexes;

    private final Map<String, Permission.Descriptor> permissions;

    public ObjectSchemaMapper(final Builder<T> builder) {

        super(ObjectSchema.Builder.class, builder);
        this.indexes = Immutable.map(builder.indexes);
        this.permissions = Immutable.map(builder.permissions);
    }

    public static <T> Builder<T> builder(final MappingContext context, final Name name, final TypeContext type) {

        return new Builder<>(context, name, type);
    }

    @Override
    public Schema.Builder<?, ?> schemaBuilder() {

        return addMembers(ObjectSchema.builder()
                .setConcrete(concrete ? null : false)
//                .setExtend(extend)
                .setIndexes(indexes.isEmpty() ? null : indexes));
    }

    @Override
    public void addLink(final ObjectSchema.Builder builder, final String name, final Link.Builder link) {

        builder.setLink(name, link);
    }

    @Override
    public void addTransient(final ObjectSchema.Builder builder, final String name, final Transient.Builder trans) {

        builder.setTransient(name, trans);
    }

    @Data
    @Accessors(chain = true)
    public static class Builder<T> implements LinkableSchemaMapper.Builder<ObjectSchema.Builder, T> {

        private final MappingContext context;

        private final Name name;

        private final TypeContext type;

        private String description;

        private List<Bucketing> bucketing;

        private Map<String, Index.Descriptor> indexes;

        private Map<String, Permission.Descriptor> permissions;

        @Override
        public ObjectSchemaMapper<T> build() {

            return new ObjectSchemaMapper<>(this);
        }
    }
}

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
import io.basestar.schema.StructSchema;
import io.basestar.type.TypeContext;
import io.basestar.util.Name;
import lombok.Data;
import lombok.experimental.Accessors;

public class StructSchemaMapper<T> extends InstanceSchemaMapper<StructSchema.Builder, T> {

    public StructSchemaMapper(final Builder<T> builder) {

        super(StructSchema.Builder.class, builder);
    }

    public static <T> Builder<T> builder(final MappingContext context, final Name name, final TypeContext type) {

        return new Builder<>(context, name, type);
    }

    @Override
    public StructSchema.Builder schemaBuilder() {

        return addMembers(StructSchema.builder()
//                .setConcrete(concrete ? null : false)
                .setExtend(extend));
    }

    @Data
    @Accessors(chain = true)
    public static class Builder<T> implements InstanceSchemaMapper.Builder<StructSchema.Builder, T> {

        private final MappingContext context;

        private final Name name;

        private final TypeContext type;

        private String description;

        @Override
        public StructSchemaMapper<T> build() {

            return new StructSchemaMapper<>(this);
        }
    }
}

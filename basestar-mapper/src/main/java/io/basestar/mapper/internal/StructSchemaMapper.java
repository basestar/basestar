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
import io.basestar.mapper.SchemaMapper;
import io.basestar.schema.StructSchema;
import io.basestar.type.TypeContext;
import io.basestar.util.Name;

import java.util.Map;

public class StructSchemaMapper<T> extends InstanceSchemaMapper<T, StructSchema.Builder> {

    public StructSchemaMapper(final MappingContext context, final Name name, final TypeContext type) {

        super(StructSchema.Builder.class, context, name, type);
    }

    public StructSchemaMapper(final StructSchemaMapper<T> copy, final String desscription) {

        super(copy, desscription);
    }

    @Override
    public StructSchema.Builder schema() {

        return addMembers(StructSchema.builder());
    }

    @Override
    public SchemaMapper<T, Map<String, Object>> withDescription(final String description) {

        return new StructSchemaMapper<>(this, description);
    }
}

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
import io.basestar.schema.Schema;
import io.basestar.type.TypeContext;
import io.basestar.util.Name;
import lombok.Data;
import lombok.experimental.Accessors;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class EnumSchemaMapper<T extends Enum<?>> implements SchemaMapper<T, String> {

    private final Name name;

    private final Class<T> erasedType;

    private final T[] constants;

    private final String description;

    private EnumSchemaMapper(final Builder<T> builder) {

        this.name = builder.name;
        this.erasedType = builder.type.erasedType();
        this.constants = builder.type.enumConstants();
        this.description = builder.description;
    }

    public static <T extends Enum<?>> Builder<T> builder(final MappingContext context, final Name name, final TypeContext type) {

        return new Builder<>(context, name, type);
    }

    @Override
    public Class<String> unmarshalledType() {

        return String.class;
    }

    @Override
    public Class<T> marshalledType() {

        return erasedType;
    }

    @Override
    public Name qualifiedName() {

        return name;
    }

    @Override
    public Schema.Builder<?, ?, String> schemaBuilder() {

        final List<String> values = Arrays.stream(constants).map(Enum::name).collect(Collectors.toList());
        return io.basestar.schema.EnumSchema.builder()
                .setDescription(description)
                .setValues(values);
    }

    @Override
    public T marshall(final Object source) {

        if(source == null) {
            return null;
        } else if(source instanceof String) {
            final String value = (String)source;
            return Arrays.stream(constants)
                    .filter(v -> v.name().equalsIgnoreCase(value))
                    .findFirst().orElse(null);
        } else {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public String unmarshall(final T value) {

        return value == null ? null : value.name();
    }

    @Override
    public Set<Class<?>> dependencies() {

        return Collections.emptySet();
    }

    @Override
    public Set<Name> namedDependencies() {

        return Collections.emptySet();
    }

    @Data
    @Accessors(chain = true)
    public static class Builder<T extends Enum<?>> implements SchemaMapper.Builder<T, String> {

        private final MappingContext context;

        private final Name name;

        private final TypeContext type;

        private String description;

        @Override
        public EnumSchemaMapper<T> build() {

            return new EnumSchemaMapper<>(this);
        }
    }
}

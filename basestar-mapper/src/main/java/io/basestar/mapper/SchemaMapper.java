package io.basestar.mapper;

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

import io.basestar.schema.Schema;
import io.basestar.type.TypeContext;
import io.basestar.util.Name;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public interface SchemaMapper<T, O> extends Serializable {

    Class<T> marshalledType();

    Class<O> unmarshalledType();

    Name qualifiedName();

    default String name() {

        return qualifiedName().toString();
    }

    Schema.Builder<?, ?, ?> schemaBuilder();

    T marshall(Object value);

    O unmarshall(T value);

    default List<T> marshall(final Collection<?> values) {

        return values.stream().map(this::marshall).collect(Collectors.toList());
    }

    default List<O> unmarshall(final Collection<? extends T> values) {

        return values.stream().map(this::unmarshall).collect(Collectors.toList());
    }

    Set<Class<?>> dependencies();

    Set<Name> namedDependencies();

    interface Builder<T, O> {

        MappingContext getContext();

        Name getName();

        TypeContext getType();

        Builder<T, O> setDescription(String description);

        String getDescription();

        SchemaMapper<T, O> build();
    }
}

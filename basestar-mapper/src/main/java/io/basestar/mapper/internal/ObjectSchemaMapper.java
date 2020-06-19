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
import io.basestar.mapper.annotation.Index;
import io.basestar.schema.ObjectSchema;
import io.basestar.type.AnnotationContext;
import io.basestar.type.TypeContext;
import io.basestar.util.Path;
import io.basestar.util.Sort;

import java.util.*;
import java.util.stream.Collectors;

public class ObjectSchemaMapper<T> extends InstanceSchemaMapper<T, ObjectSchema.Builder> {

    private final Map<String, io.basestar.schema.Index.Builder> indexes = new HashMap<>();

    public ObjectSchemaMapper(final MappingContext context, final String name, final TypeContext type) {

        super(context, name, type, ObjectSchema.Builder.class);
        for(final AnnotationContext<Index> annot : type.annotations(Index.class)) {
            final Index index = annot.annotation();
            indexes.put(index.name(), new io.basestar.schema.Index.Builder()
                    .setPartition(partition(index.partition()))
                    .setSort(sort(index.sort()))
                    .setProjection(projection(index.projection()))
                    .setUnique(index.unique())
                    .setOver(over(index.over())));
        }
    }

    private Map<String, Path> over(final Index.Over[] over) {

        return Arrays.stream(over).collect(Collectors.toMap(
                Index.Over::as,
                v -> Path.parse(v.path())
        ));
    }

    private Set<String> projection(final String[] projection) {

        return new HashSet<>(Arrays.asList(projection));
    }

    private List<Sort> sort(final String[] sort) {

        return Arrays.stream(sort).map(Sort::parse).collect(Collectors.toList());
    }

    private List<Path> partition(final String[] partition) {

        return Arrays.stream(partition).map(Path::parse).collect(Collectors.toList());
    }

    @Override
    public ObjectSchema.Builder schema() {

        return addMembers(ObjectSchema.builder()
                .setIndexes(indexes));
    }
}

package io.basestar.mapper.annotation;

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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.basestar.mapper.MappingContext;
import io.basestar.mapper.internal.AnnotationUtils;
import io.basestar.mapper.internal.ObjectSchemaMapper;
import io.basestar.mapper.internal.annotation.SchemaModifier;
import io.basestar.schema.Consistency;
import io.basestar.type.AnnotationContext;
import io.basestar.util.Name;
import io.basestar.util.Sort;
import lombok.RequiredArgsConstructor;

import java.lang.annotation.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
@Repeatable(Index.Multi.class)
@SchemaModifier(Index.Modifier.class)
public @interface Index {

    String name();

    String[] partition() default {};

    String[] sort() default {};

    boolean unique() default false;

    Over[] over() default {};

    String[] projection() default {};

    Consistency consistency() default Consistency.NONE;

    @Documented
    @Retention(RetentionPolicy.RUNTIME)
    @Target({ElementType.TYPE})
    @SchemaModifier(Index.Multi.Modifier.class)
    @interface Multi {

        Index[] value();

        @RequiredArgsConstructor
        class Modifier implements SchemaModifier.Modifier<ObjectSchemaMapper.Builder<?>> {

            private final Multi annotation;

            @Override
            public void modify(final MappingContext context, final ObjectSchemaMapper.Builder<?> mapper) {

                Index.Modifier.modify(mapper, annotation.value());
            }
        }
    }

    @Documented
    @Retention(RetentionPolicy.RUNTIME)
    @interface Over {

        String as();

        String path();
    }

    @RequiredArgsConstructor
    class Modifier implements SchemaModifier.Modifier<ObjectSchemaMapper.Builder<?>> {

        private final Index annotation;

        @Override
        public void modify(final MappingContext context, final ObjectSchemaMapper.Builder<?> mapper) {

            modify(mapper, annotation);
        }

        private static void modify(final ObjectSchemaMapper.Builder<?> mapper, final Index ... annotations) {

            final Map<String, io.basestar.schema.Index.Descriptor> indexes = new HashMap<>();
            Arrays.stream(annotations).forEach(annotation -> indexes.put(annotation.name(), new io.basestar.schema.Index.Builder()
                    .setPartition(Name.parseList(annotation.partition()))
                    .setSort(Sort.parseList(annotation.sort()))
                    .setProjection(ImmutableSet.copyOf(annotation.projection()))
                    .setUnique(annotation.unique())
                    .setConsistency(consistency(annotation.consistency()))
                    .setOver(over(annotation.over()))));
            mapper.setIndexes(indexes);
        }

        private static Consistency consistency(final Consistency consistency) {

            return consistency == Consistency.NONE ? null : consistency;
        }

        private static Map<String, io.basestar.util.Name> over(final Over[] over) {

            return Arrays.stream(over).collect(Collectors.toMap(
                    Over::as, v -> io.basestar.util.Name.parse(v.path())
            ));
        }

        public static Index annotation(final io.basestar.schema.Index index) {

            final Over[] over = index.getOver().entrySet().stream()
                    .map(entry -> new AnnotationContext<>(Over.class, ImmutableMap.of("as", entry.getKey(), "path", entry.getValue().toString()))
                            .annotation()).toArray(Over[]::new);

            return new AnnotationContext<>(Index.class, ImmutableMap.<String, Object>builder()
                    .put("name", index.getName())
                    .put("partition", AnnotationUtils.stringArray(index.getPartition()))
                    .put("sort", AnnotationUtils.stringArray(index.getSort()))
                    .put("unique", index.isUnique())
                    .put("over", over)
                    .put("projection", AnnotationUtils.stringArray(index.getProjection()))
                    .put("consistency", index.getConsistency(Consistency.NONE))
                    .build()).annotation();
        }
    }
}

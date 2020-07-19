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
import io.basestar.mapper.MappingContext;
import io.basestar.mapper.SchemaMapper;
import io.basestar.mapper.internal.AnnotationUtils;
import io.basestar.mapper.internal.TypeMapper;
import io.basestar.mapper.internal.ViewSchemaMapper;
import io.basestar.mapper.internal.annotation.SchemaDeclaration;
import io.basestar.type.AnnotationContext;
import io.basestar.type.TypeContext;
import io.basestar.util.Name;
import lombok.RequiredArgsConstructor;

import java.lang.annotation.*;
import java.util.Set;

@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@SchemaDeclaration(ViewSchema.Declaration.class)
public @interface ViewSchema {

    @Documented
    @Retention(RetentionPolicy.RUNTIME)
    @Target({})
    @interface From {

        Class<?> value() default Object.class;

        String schema() default "";

        String[] expand() default {};
    }

    String INFER_NAME = "";

    String name() default INFER_NAME;

    boolean materialized() default false;

    From from();

    @RequiredArgsConstructor
    class Declaration implements SchemaDeclaration.Declaration {

        private final ViewSchema annotation;

        @Override
        public Name getQualifiedName(final TypeContext type) {

            return Name.parse(annotation.name().equals(INFER_NAME) ? type.simpleName() : annotation.name());
        }

        @Override
        public SchemaMapper<?, ?> mapper(final MappingContext context, final TypeContext type) {

            final From from = annotation.from();
            final Name fromSchema;
            if(!from.schema().isEmpty()) {
                fromSchema = Name.parse(from.schema());
            } else {
                final TypeMapper.OfCustom tmp = new TypeMapper.OfCustom(context, TypeContext.from(from.value()));
                fromSchema = tmp.getQualifiedName();
            }
            final Set<Name> fromExpand = Name.parseSet(from.expand());
            final boolean materialized = annotation.materialized();
            return new ViewSchemaMapper<>(context, getQualifiedName(type), type, fromSchema, fromExpand, materialized);
        }

        public static ViewSchema annotation(final io.basestar.schema.ViewSchema schema) {

            final From from = new AnnotationContext<>(From.class, ImmutableMap.<String, Object>builder()
                    .put("value", Object.class)
                    .put("schema", schema.getFrom().getSchema().getQualifiedName().toString())
                    .put("expand", AnnotationUtils.stringArray(schema.getFrom().getExpand()))
                    .build()).annotation();
            return new AnnotationContext<>(ViewSchema.class, ImmutableMap.<String, Object>builder()
                    .put("name", schema.getQualifiedName().toString())
                    .put("materialized", schema.isMaterialized())
                    .put("from", from)
                    .build()).annotation();
        }
    }
}

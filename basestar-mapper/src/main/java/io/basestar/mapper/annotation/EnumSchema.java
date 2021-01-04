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
import io.basestar.mapper.MappingStrategy;
import io.basestar.mapper.SchemaMapper;
import io.basestar.mapper.internal.EnumSchemaMapper;
import io.basestar.mapper.internal.annotation.SchemaDeclaration;
import io.basestar.type.AnnotationContext;
import io.basestar.type.TypeContext;
import io.basestar.util.Name;
import lombok.RequiredArgsConstructor;

import java.lang.annotation.*;

@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@SchemaDeclaration(EnumSchema.Declaration.class)
public @interface EnumSchema {

    String name() default MappingStrategy.INFER_NAME;

    @RequiredArgsConstructor
    class Declaration implements SchemaDeclaration.Declaration {

        private final EnumSchema annotation;

        @Override
        public Name getQualifiedName(final MappingContext context, final TypeContext type) {

            return context.strategy().schemaName(context, annotation.name(), type);
        }

        @Override
        public SchemaMapper.Builder<?, ?> mapper(final MappingContext context, final TypeContext type) {

            return EnumSchemaMapper.builder(context, getQualifiedName(context, type), type);
        }

        public static EnumSchema annotation(final io.basestar.schema.EnumSchema schema) {

            return new AnnotationContext<>(EnumSchema.class, ImmutableMap.<String, Object>builder()
                    .put("name", schema.getQualifiedName().toString())
                    .build()).annotation();
        }
    }
}

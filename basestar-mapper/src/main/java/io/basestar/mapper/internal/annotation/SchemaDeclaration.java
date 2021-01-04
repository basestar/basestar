package io.basestar.mapper.internal.annotation;

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
import io.basestar.mapper.internal.EnumSchemaMapper;
import io.basestar.mapper.internal.StructSchemaMapper;
import io.basestar.type.TypeContext;
import io.basestar.util.Name;

import java.lang.annotation.*;

@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.ANNOTATION_TYPE)
public @interface SchemaDeclaration {

    Class<? extends Declaration> value();

    interface Declaration {

        Name getQualifiedName(MappingContext context, TypeContext type);

        SchemaMapper.Builder<?, ?> mapper(MappingContext context, TypeContext type);

        class Basic implements Declaration {

            public static final Basic INSTANCE = new Basic();

            @Override
            public Name getQualifiedName(final MappingContext context, final TypeContext type) {

                return context.strategy().schemaName(context, type);
            }

            @Override
            public SchemaMapper.Builder<?, ?> mapper(final MappingContext context, final TypeContext type) {

                final Name name = getQualifiedName(context, type);
                if (type.isEnum()) {
                    return EnumSchemaMapper.builder(context, name, type);
                } else {
                    return StructSchemaMapper.builder(context, name, type);
                }
            }
        }
    }
}

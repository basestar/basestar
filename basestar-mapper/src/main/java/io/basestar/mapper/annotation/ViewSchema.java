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

import io.basestar.mapper.MappingContext;
import io.basestar.mapper.SchemaMapper;
import io.basestar.mapper.internal.ViewSchemaMapper;
import io.basestar.mapper.internal.annotation.SchemaDeclaration;
import io.basestar.type.TypeContext;
import io.basestar.util.AbstractPath;
import io.basestar.util.Name;
import lombok.RequiredArgsConstructor;

import java.lang.annotation.*;
import java.util.Set;

@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@SchemaDeclaration(ViewSchema.Declaration.class)
public @interface ViewSchema {

    String INFER_NAME = "";

    String name() default INFER_NAME;

    String from();

    String[] expand() default {};

    boolean materialized() default false;

    @RequiredArgsConstructor
    class Declaration implements SchemaDeclaration.Declaration {

        private final ViewSchema annotation;

        @Override
        public SchemaMapper<?, ?> mapper(final MappingContext context, final TypeContext type) {

            final String name = annotation.name().equals(INFER_NAME) ? type.simpleName() : annotation.name();
            final Name fromSchema = Name.parse(annotation.from());
            final boolean materialized = annotation.materialized();
            final Set<Name> fromExpand = Name.parseSet(annotation.expand());
//            final Expression where = annotation.where().isEmpty() ? null : Expression.parse(annotation.where());
//            final List<String> group = Arrays.asList(annotation.group());
            return new ViewSchemaMapper<>(context, Name.parse(name), type, fromSchema, fromExpand, materialized);
        }

        public static ViewSchema from(final io.basestar.schema.ViewSchema schema) {

            return new ViewSchema() {

                @Override
                public Class<? extends Annotation> annotationType() {

                    return ViewSchema.class;
                }

                @Override
                public String name() {

                    return schema.getQualifiedName().toString();
                }

                @Override
                public String from() {

                    return schema.getFrom().getSchema().getQualifiedName().toString();
                }

                @Override
                public String[] expand() {

                    // FIXME
                    return schema.getFrom().getExpand().isEmpty() ? null : schema.getFrom().getExpand().stream().map(AbstractPath::toString).toArray(String[]::new);
                }

                @Override
                public boolean materialized() {

                    return schema.isMaterialized();
                }
            };
        }
    }
}

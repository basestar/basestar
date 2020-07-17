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
import io.basestar.mapper.internal.LinkMapper;
import io.basestar.mapper.internal.MemberMapper;
import io.basestar.mapper.internal.annotation.MemberDeclaration;
import io.basestar.type.PropertyContext;
import lombok.RequiredArgsConstructor;

import java.lang.annotation.*;

@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD, ElementType.METHOD})
@MemberDeclaration(Link.Declaration.class)
public @interface Link {

    String INFER_NAME = "";

    String name() default INFER_NAME;

    @RequiredArgsConstructor
    class Declaration implements MemberDeclaration.Declaration {

        private final Link annotation;

        @Override
        public MemberMapper<?> mapper(final MappingContext context, final PropertyContext prop) {

            final String name = INFER_NAME.equals(annotation.name()) ? prop.simpleName() : annotation.name();
            return new LinkMapper(context, name, prop);
        }

        public static Link from(final io.basestar.schema.Link link) {

            return new Link() {

                @Override
                public Class<? extends Annotation> annotationType() {

                    return Link.class;
                }

                @Override
                public String name() {

                    return link.getName();
                }
            };
        }
    }
}

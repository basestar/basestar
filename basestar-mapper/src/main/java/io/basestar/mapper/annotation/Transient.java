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

import io.basestar.expression.Expression;
import io.basestar.mapper.MappingContext;
import io.basestar.mapper.internal.MemberMapper;
import io.basestar.mapper.internal.PropertyMapper;
import io.basestar.mapper.internal.annotation.MemberDeclaration;
import io.basestar.type.PropertyContext;
import lombok.RequiredArgsConstructor;

import java.lang.annotation.*;

@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD, ElementType.METHOD})
@MemberDeclaration(Transient.Declaration.class)
public @interface Transient {

    String INFER_NAME = "";

    String name() default INFER_NAME;

    String expression() default "";

    String[] expand() default {};

    @RequiredArgsConstructor
    class Declaration implements MemberDeclaration.Declaration {

        private final Transient annotation;

        @Override
        public MemberMapper<?> mapper(final MappingContext context, final PropertyContext prop) {

            //FIXME
            final String name = INFER_NAME.equals(annotation.name()) ? prop.simpleName() : annotation.name();
            final Expression expression = annotation.expression().isEmpty() ? null : Expression.parse(annotation.expression());
            return new PropertyMapper(context, name, prop, expression);
        }
    }
}

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

import io.basestar.mapper.context.PropertyContext;
import io.basestar.mapper.internal.annotation.BindSchema;
import io.basestar.schema.InstanceSchema;
import lombok.RequiredArgsConstructor;

import java.lang.annotation.*;

@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD, ElementType.METHOD})
@BindSchema(Transient.Binder.class)
public @interface Transient {

    String INFER_NAME = "";

    String name() default INFER_NAME;

    String expression() default "";

    String[] expand() default {};

    @RequiredArgsConstructor
    class Binder implements BindSchema.Handler {

        private final Transient annotation;

        @Override
        public String name(final PropertyContext property) {

            return property.name();
        }

        @Override
        public void addToSchema(final InstanceSchema.Builder parent, final PropertyContext prop) {

            if(parent instanceof io.basestar.schema.ObjectSchema.Builder) {
                final String name = INFER_NAME.equals(annotation.name()) ? prop.simpleName() : annotation.name();
                final io.basestar.schema.Transient.Builder builder = new io.basestar.schema.Transient.Builder();
                ((io.basestar.schema.ObjectSchema.Builder)parent).setTransient(name, builder);
            } else {
                throw new IllegalStateException("transients only allowed on object schemas");
            }
        }
    }
}

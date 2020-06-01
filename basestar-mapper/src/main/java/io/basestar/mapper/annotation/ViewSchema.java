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

import io.basestar.mapper.context.TypeContext;
import io.basestar.mapper.internal.annotation.BindNamespace;
import io.basestar.schema.Schema;
import lombok.RequiredArgsConstructor;

import java.lang.annotation.*;

@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@BindNamespace(ViewSchema.Binder.class)
public @interface ViewSchema {

    String INFER_NAME = "";

    String name() default INFER_NAME;

    @RequiredArgsConstructor
    class Binder implements BindNamespace.Handler {

        private final ViewSchema annotation;

        @Override
        public String name(final TypeContext type) {

            final String name = annotation.name();
            return name.equals(INFER_NAME) ? type.simpleName() : name;
        }

        @Override
        public Schema.Builder<?> schemaBuilder(final TypeContext type) {

            return io.basestar.schema.ViewSchema.builder();
        }
    }
}

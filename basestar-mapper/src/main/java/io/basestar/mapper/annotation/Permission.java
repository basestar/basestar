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

import java.lang.annotation.*;

@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
@Repeatable(Permission.Multi.class)
public @interface Permission {

    @SuppressWarnings("unused")
    String READ = io.basestar.schema.Permission.READ;

    @SuppressWarnings("unused")
    String CREATE = io.basestar.schema.Permission.CREATE;

    @SuppressWarnings("unused")
    String UPDATE = io.basestar.schema.Permission.UPDATE;

    @SuppressWarnings("unused")
    String DELETE = io.basestar.schema.Permission.DELETE;

    String on();

    boolean anon() default false;

    String expression() default "";

    String[] expand() default {};

    @Documented
    @Retention(RetentionPolicy.RUNTIME)
    @Target({ElementType.TYPE})
    @interface Multi {

        Permission[] value();
    }
}

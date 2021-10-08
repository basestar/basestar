package io.basestar.type.has;

/*-
 * #%L
 * basestar-core
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

import io.basestar.type.MethodContext;

import java.util.List;
import java.util.Optional;

public interface HasMethods {

    List<MethodContext> declaredMethods();

    List<MethodContext> methods();

    default Optional<MethodContext> method(final String name, final Class<?>... args) {

        return methods().stream().filter(HasName.match(name)).filter(HasParameters.match(args)).findFirst();
    }

    default Optional<MethodContext> declaredMethod(final String name, final Class<?>... args) {

        return declaredMethods().stream().filter(HasName.match(name)).filter(HasParameters.match(args)).findFirst();
    }
}

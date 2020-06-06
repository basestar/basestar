package io.basestar.type.has;

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

import io.basestar.type.ParameterContext;

import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public interface HasParameters {

    List<ParameterContext> parameters();

    default List<Class<?>> erasedParameterTypes() {

        return parameters().stream().map(HasType::erasedType)
                .collect(Collectors.toList());
    }

    static <T extends HasParameters> Predicate<T> match(final Class<?> ... raw) {

        return match(Arrays.asList(raw));
    }

    static <T extends HasParameters> Predicate<T> match(final List<Class<?>> raw) {

        return v -> v.erasedParameterTypes().equals(raw);
    }

    static <T extends HasParameters> Predicate<T> match(final int count) {

        return v -> v.parameters().size() == count;
    }

}

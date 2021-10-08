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

import io.basestar.type.TypeContext;
import io.leangen.geantyref.GenericTypeReflector;

import java.lang.reflect.AnnotatedType;
import java.lang.reflect.Type;
import java.util.function.Predicate;

public interface HasType {

    AnnotatedType annotatedType();

    default TypeContext type() {

        return TypeContext.from(annotatedType());
    }

    default Type genericType() {

        return annotatedType().getType();
    }

    @SuppressWarnings("unchecked")
    default <V> Class<V> erasedType() {

        return (Class<V>) GenericTypeReflector.erase(genericType());
    }

    static Predicate<HasType> match(final Class<?> raw) {

        return v -> raw.equals(v.erasedType());
    }

    static Predicate<HasType> match(final Predicate<TypeContext> predicate) {

        return v -> predicate.test(v.type());
    }
}

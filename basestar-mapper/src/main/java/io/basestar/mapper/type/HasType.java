package io.basestar.mapper.type;

/*-
 * #%L
 * basestar-mapper
 * $Id:$
 * $HeadURL:$
 * %%
 * Copyright (C) 2020 basestar.io
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

import io.leangen.geantyref.GenericTypeReflector;

import java.lang.reflect.AnnotatedType;
import java.lang.reflect.Type;
import java.util.function.Predicate;

public interface HasType<V> {

    AnnotatedType annotatedType();

    default WithType<V> type() {

        return WithType.with(annotatedType());
    }

    default Type genericType() {

        return annotatedType().getType();
    }

    @SuppressWarnings("unchecked")
    default Class<V> erasedType() {

        return (Class<V>)GenericTypeReflector.erase(genericType());
    }

    static Predicate<HasType<?>> match(final Class<?> raw) {

        return v -> raw.equals(v.erasedType());
    }

    static <V> Predicate<HasType<V>> match(final Predicate<WithType<V>> predicate) {

        return v -> predicate.test(v.type());
    }
}

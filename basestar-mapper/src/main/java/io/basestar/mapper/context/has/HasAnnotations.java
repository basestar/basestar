package io.basestar.mapper.context.has;

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

import io.basestar.mapper.context.AnnotationContext;

import java.lang.annotation.Annotation;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public interface HasAnnotations {

    List<AnnotationContext<?>> annotations();

    @SuppressWarnings("unchecked")
    default <A extends Annotation> AnnotationContext<A> annotation(final Class<A> of) {

        return (AnnotationContext<A>)annotations().stream().filter(v -> v.erasedType().equals(of))
                .findFirst().orElse(null);
    }

    @SuppressWarnings("unchecked")
    default <A extends Annotation> List<AnnotationContext<A>> annotations(final Class<A> of) {

        return annotations().stream()
                .filter(v -> v.erasedType().equals(of))
                .map(v -> (AnnotationContext<A>)v)
                .collect(Collectors.toList());
    }

    static Predicate<HasAnnotations> match(final Class<? extends Annotation> of) {

        return v -> v.annotation(of) != null;
    }
}
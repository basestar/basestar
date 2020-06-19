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

import io.basestar.type.AnnotationContext;

import java.lang.annotation.Annotation;
import java.lang.annotation.Repeatable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public interface HasAnnotations {

    List<AnnotationContext<?>> annotations();

    @SuppressWarnings("unchecked")
    default <A extends Annotation> AnnotationContext<A> annotation(final Class<A> of) {

        return (AnnotationContext<A>)annotations().stream().filter(v -> v.erasedType().equals(of))
                .findFirst().orElse(null);
    }

    @SuppressWarnings("unchecked")
    default <A extends Annotation> List<AnnotationContext<A>> annotations(final Class<A> of) {

        final Repeatable rep = of.getAnnotation(Repeatable.class);
        final Class<? extends Annotation> repType = rep == null ? null : rep.value();
        return annotations().stream()
                .flatMap(v -> {
                    final Class<?> erased = v.erasedType();
                    if(erased.equals(of)) {
                        return Stream.of((AnnotationContext<A>)v);
                    } else if(erased.equals(repType)) {
                        try {
                            final Method value = repType.getMethod("value");
                            final A[] as = (A[])value.invoke(v.annotation());
                            return Arrays.stream(as).map(AnnotationContext::new);
                        } catch (final NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
                            throw new IllegalStateException(e);
                        }
                    } else {
                        return Stream.empty();
                    }
                })
                .collect(Collectors.toList());
    }

    static Predicate<HasAnnotations> match(final Class<? extends Annotation> of) {

        return v -> v.annotation(of) != null;
    }
}

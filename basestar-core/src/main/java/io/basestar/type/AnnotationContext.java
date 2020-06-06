package io.basestar.type;

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

import io.basestar.type.has.HasType;
import io.leangen.geantyref.GenericTypeReflector;
import lombok.Getter;
import lombok.experimental.Accessors;

import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.AnnotatedType;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Getter
@Accessors(fluent = true)
public class AnnotationContext<A extends Annotation> implements HasType {

    private final A annotation;

    protected AnnotationContext(final A annotation) {

        this.annotation = annotation;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <V> Class<V> erasedType() {

        return (Class<V>)annotation.annotationType();
    }

    @Override
    public AnnotatedType annotatedType() {

        return GenericTypeReflector.annotate(erasedType());
    }

    public static List<AnnotationContext<?>> from(final AnnotatedElement element) {

        return Arrays.stream(element.getAnnotations())
                .map(AnnotationContext::new)
                .collect(Collectors.toList());
    }
}

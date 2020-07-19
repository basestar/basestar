package io.basestar.codegen.model;

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

import com.google.common.collect.ImmutableMap;
import io.basestar.codegen.CodegenSettings;
import io.basestar.type.AnnotationContext;
import io.basestar.util.Name;

import java.lang.annotation.Annotation;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

@SuppressWarnings("unused")
public class AnnotationModel<A extends Annotation> extends Model {

    private final A annotation;

    private final Map<String, Object> override;

    public AnnotationModel(final CodegenSettings settings, final A annotation) {

        this(settings, annotation, ImmutableMap.of());
    }

    public AnnotationModel(final CodegenSettings settings, final A annotation, final Map<String, Object> override) {

        super(settings);
        this.annotation = annotation;
        this.override = override;
    }

    public String getClassName() {

        return annotation.annotationType().getCanonicalName();
    }

    public Map<String, Object> getValues() {

        final AnnotationContext<A> context =  new AnnotationContext<>(annotation);
        final Map<String, Object> values = new HashMap<>(context.nonDefaultValues());
        values.putAll(override);

        return values.entrySet().stream().filter(e -> e.getValue() != null)
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> {
                    final Object value = entry.getValue();
                    if(value instanceof Annotation) {
                        return new AnnotationModel<>(getSettings(), (Annotation)value);
                    } else if(value instanceof Class<?>) {
                        return wrapClassName((Class<?>)value);
                    } else {
                        return value;
                    }
                }));
    }

    // Convert classes to objects with a toString method for freemarker

    public static Object wrapClassName(final Class<?> cls) {

        return new Object() {
            @Override
            public String toString() {

                return cls.getCanonicalName() + ".class";
            }
        };
    }

    public static Object wrapClassName(final Name name) {

        return new Object() {
            @Override
            public String toString() {

                return name.toString() + ".class";
            }
        };
    }
}

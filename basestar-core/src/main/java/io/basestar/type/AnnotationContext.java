package io.basestar.type;

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

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.basestar.type.has.HasMethods;
import io.basestar.type.has.HasType;
import io.leangen.geantyref.AnnotationFormatException;
import io.leangen.geantyref.GenericTypeReflector;
import io.leangen.geantyref.TypeFactory;
import lombok.Getter;
import lombok.experimental.Accessors;

import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.AnnotatedType;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.stream.Collectors;

@Getter
@Accessors(fluent = true)
@SuppressWarnings("Guava")
public class AnnotationContext<A extends Annotation> implements HasType, HasMethods {

    String VALUE = "value";

    // Methods that are not annotation values
    private static final Set<String> NOT_VALUES = ImmutableSet.of("hashCode", "annotationType", "toString");

    private final Class<A> annotationType;

    private final Supplier<A> annotation;

    private final Supplier<Map<String, Object>> values;

    private final Supplier<Map<String, Object>> defaultValues;

    public AnnotationContext(final Class<A> annotationType, final Map<String, Object> values) {

        this.annotationType = annotationType;
        final Map<String, Object> valuesCopy = ImmutableMap.copyOf(values);
        this.annotation = Suppliers.memoize(() -> {
            try {
                return TypeFactory.annotation(annotationType, values);
            } catch (final AnnotationFormatException e) {
                throw new IllegalStateException(e);
            }
        });
        this.values = () -> valuesCopy;
        this.defaultValues = Suppliers.memoize(() -> defaultValues(type()));
    }

    @SuppressWarnings("unchecked")
    public AnnotationContext(final A annotation) {

        this.annotationType = (Class<A>) annotation.annotationType();
        this.annotation = () -> annotation;
        this.values = Suppliers.memoize(() -> {
            final Map<String, Object> values = new HashMap<>();
            final TypeContext context = type();
            context.methods().forEach(m -> {
                if (m.parameters().size() == 0 && !m.isStatic() && !NOT_VALUES.contains(m.name())) {
                    try {
                        values.put(m.name(), m.invoke(annotation));
                    } catch (final InvocationTargetException | IllegalAccessException | IllegalArgumentException e) {
                        throw new IllegalStateException(e);
                    }
                }
            });
            return values;
        });
        this.defaultValues = Suppliers.memoize(() -> defaultValues(type()));
    }

    private static Map<String, Object> defaultValues(final TypeContext context) {

        final Map<String, Object> values = new HashMap<>();
        context.methods().forEach(m -> {
            if (m.parameters().size() == 0 && !m.isStatic() && !NOT_VALUES.contains(m.name())) {
                values.put(m.name(), m.method().getDefaultValue());
            }
        });
        return values;
    }

    public A annotation() {

        return annotation.get();
    }

    public <T> T value() {

        return value(VALUE);
    }

    @SuppressWarnings("unchecked")
    public <T> T value(final String name) {

        return (T) values.get().get(name);
    }

    public Optional<TypeContext> valueType() {

        return valueType(VALUE);
    }

    public Optional<TypeContext> valueType(final String name) {

        return type().method(name).map(HasType::type);
    }

    @SuppressWarnings("unchecked")
    public <T> Optional<T> nonDefaultValue(final String name) {

        final Map<String, Object> values = values();
        final Map<String, Object> defaults = defaultValues();
        if (values.containsKey(name)) {
            final T value = (T) values.get(name);
            final Object def = defaults.get(name);
            if (!Objects.deepEquals(value, def)) {
                return Optional.of(value);
            }
        }
        return Optional.empty();
    }

    public Map<String, Object> values() {

        return values.get();
    }

    public Map<String, Object> defaultValues() {

        return defaultValues.get();
    }

    public Map<String, Object> nonDefaultValues() {

        final Map<String, Object> defaultValues = defaultValues();
        return values().entrySet().stream()
                .filter(e -> e.getValue() != null)
                .filter(e -> !Objects.deepEquals(e.getValue(), defaultValues.get(e.getKey())))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    @Override
    @SuppressWarnings("unchecked")
    public <V> Class<V> erasedType() {

        return (Class<V>) annotationType;
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

    @Override
    public List<MethodContext> declaredMethods() {

        return type().declaredMethods();
    }

    @Override
    public List<MethodContext> methods() {

        return type().methods();
    }
}

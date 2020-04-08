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
import lombok.Data;
import lombok.Getter;
import lombok.experimental.Accessors;

import java.lang.reflect.AnnotatedType;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Getter
@Accessors(fluent = true)
public class WithType<T> implements HasName, HasModifiers, HasAnnotations,
        HasTypeParameters, HasType<T>, HasMethods<T>, HasFields<T> {

    private static final ConcurrentMap<AnnotatedType, WithType<?>> CACHE = new ConcurrentHashMap<>();

    private final AnnotatedType annotatedType;

    private final Class<T> rawType;

    private final WithType<? super T> superclass;

    private final List<WithType<? super T>> interfaces;

    private final List<WithConstructor<T>> declaredConstructors;

    private final List<WithField<T, ?>> declaredFields;

    private final List<WithMethod<T, ?>> declaredMethods;

    private final List<WithField<? super T, ?>> fields;

    private final List<WithMethod<? super T, ?>> methods;

    private final List<WithProperty<T, ?>> properties;

    private final List<WithTypeVariable<?>> typeParameters;

    private final List<WithAnnotation<?>> annotations;

    @SuppressWarnings("unchecked")
    private WithType(final AnnotatedType type) {

        this.annotatedType = type;
        this.rawType = (Class<T>)GenericTypeReflector.erase(type.getType());

        final Class<?> sc = GenericTypeReflector.erase(type.getType()).getSuperclass();

        this.superclass = sc == null ? null : WithType.with(GenericTypeReflector.getExactSuperType(type, sc));

        this.interfaces = Arrays.stream(rawType.getInterfaces())
                .map(ifc -> WithType.with(GenericTypeReflector.getExactSuperType(type, ifc)))
                .collect(Collectors.toList());

        this.declaredConstructors = Arrays.stream(rawType.getDeclaredConstructors())
                .map(v -> new WithConstructor<>(this, (java.lang.reflect.Constructor<T>)v))
                .collect(Collectors.toList());

        this.declaredFields = Arrays.stream(rawType.getDeclaredFields())
                .map(v -> new WithField<>(this, v))
                .collect(Collectors.toList());

        this.declaredMethods = Arrays.stream(rawType.getDeclaredMethods())
                .map(v -> new WithMethod<>(this, v))
                .collect(Collectors.toList());

        final Map<String, WithField<? super T, ?>> allFields = new HashMap<>();
        final Map<Signature, WithMethod<? super T, ?>> allMethods = new HashMap<>();

        if(superclass != null) {
            superclass.fields().forEach(f -> allFields.put(f.name(), f));
            superclass.methods().forEach(m -> allMethods.put(Signature.of(m), m));
        }
        interfaces.forEach(ifc -> {
            ifc.fields().forEach(f -> allFields.put(f.name(), f));
            ifc.methods().forEach(m -> allMethods.put(Signature.of(m), m));
        });
        declaredFields.forEach(f -> allFields.put(f.name(), f));
        declaredMethods.forEach(m -> allMethods.put(Signature.of(m), m));

        this.fields = allFields.values().stream()
                .filter(HasModifiers.match(Modifier.PUBLIC))
                .collect(Collectors.toList());

        this.methods = allMethods.values().stream()
                .filter(HasModifiers.match(Modifier.PUBLIC))
                .collect(Collectors.toList());

        final Map<String, WithField<? super T, Object>> matchedFields = allFields.values().stream()
                .filter(HasModifiers.matchNot(Modifier.STATIC))
                .map(v -> (WithField<? super T, Object>)v)
                .collect(Collectors.toMap(WithField::name, v -> v));

        final Map<String, WithMethod<? super T, Object>> matchedGetters = allMethods.values().stream()
                .filter(HasName.match(Pattern.compile("get[A-Z].*")))
                .filter(HasParameters.match(0))
                .filter(HasType.match(void.class).negate())
                .filter(HasModifiers.matchNot(Modifier.STATIC))
                .map(v -> (WithMethod<? super T, Object>)v)
                .collect(Collectors.toMap(v -> lowerCamel(v.name().substring(3)), v -> v));

        final Map<String, WithMethod<? super T, ?>> matchedSetters = allMethods.values().stream()
                .filter(HasName.match(Pattern.compile("set[A-Z].*")))
                .filter(HasParameters.match(1))
                .filter(HasModifiers.matchNot(Modifier.STATIC))
                .collect(Collectors.toMap(v -> lowerCamel(v.name().substring(3)), v -> v));

        this.properties = Stream.of(matchedFields, matchedGetters, matchedSetters)
                .map(Map::keySet).flatMap(Collection::stream)
                .distinct().map(v -> {
                    final WithField<? super T, Object> field = matchedFields.get(v);
                    final WithMethod<? super T, Object> getter = matchedGetters.get(v);
                    final WithMethod<? super T, ?> setter = matchedSetters.get(v);
                    return new WithProperty<>(this, v,field, getter, setter);
                })
                .collect(Collectors.toList());

        this.typeParameters = Arrays.stream(rawType.getTypeParameters())
                    .map(v -> new WithTypeVariable<>(v, GenericTypeReflector.getTypeParameter(type, v)))
                    .collect(Collectors.toList());

        this.annotations = WithAnnotation.from(annotatedType);
    }

    public static <T> WithType<T> with(final Class<T> type) {

        return with(GenericTypeReflector.annotate(type));
    }

    public static <T> WithType<T> with(final Type type) {

        return with(GenericTypeReflector.annotate(type));
    }

    @SuppressWarnings("unchecked")
    public static <T> WithType<T> with(final AnnotatedType type) {

        synchronized (CACHE) {
            final WithType<?> result;
            if(!CACHE.containsKey(type)) {
                result = new WithType<>(type);
                CACHE.put(type, result);
            } else {
                result = CACHE.get(type);
            }
            return (WithType<T>)result;
        }
    }

    @Override
    public int modifiers() {

        return rawType.getModifiers();
    }

    @Override
    public String name() {

        return rawType.getName();
    }

    @Override
    public String simpleName() {

        return rawType.getSimpleName();
    }

    @Override
    public WithType<T> type() {

        return this;
    }

    public boolean isEnum() {

        return rawType.isEnum();
    }

    public T[] enumConstants() {

        return rawType.getEnumConstants();
    }

    @Data
    private static class Signature {

        private final String name;

        private final List<Class<?>> args;

        public static <T> Signature of(final WithMethod<T, ?> m) {

            return new Signature(m.name(), m.erasedParameterTypes());
        }
    }

    private static String lowerCamel(final String v) {

        if(v.length() < 2) {
            return v.toLowerCase();
        } else if(Character.isUpperCase(v.charAt(1))) {
            return v;
        } else {
            return Character.toString(v.charAt(0)).toLowerCase() + v.substring(1);
        }
    }
}

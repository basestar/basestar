package io.basestar.mapper.context;

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

import io.basestar.mapper.context.has.*;
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
public class TypeContext implements HasName, HasModifiers, HasAnnotations,
        HasTypeParameters, HasType, HasMethods, HasFields {

    private static final ConcurrentMap<AnnotatedType, TypeContext> CACHE = new ConcurrentHashMap<>();

    private final AnnotatedType annotatedType;

    private final Class<?> rawType;

    private final TypeContext superclass;

    private final List<TypeContext> interfaces;

    private final List<ConstructorContext> declaredConstructors;

    private final List<FieldContext> declaredFields;

    private final List<MethodContext> declaredMethods;

    private final List<FieldContext> fields;

    private final List<MethodContext> methods;

    private final List<PropertyContext> properties;

    private final List<TypeVariableContext> typeParameters;

    private final List<AnnotationContext<?>> annotations;

    private TypeContext(final AnnotatedType type) {

        this.annotatedType = type;
        this.rawType = GenericTypeReflector.erase(type.getType());

        final Class<?> sc = GenericTypeReflector.erase(type.getType()).getSuperclass();

        this.superclass = sc == null ? null : TypeContext.from(GenericTypeReflector.getExactSuperType(type, sc));

        this.interfaces = Arrays.stream(rawType.getInterfaces())
                .map(ifc -> TypeContext.from(GenericTypeReflector.getExactSuperType(type, ifc)))
                .collect(Collectors.toList());

        this.declaredConstructors = Arrays.stream(rawType.getDeclaredConstructors())
                .map(v -> new ConstructorContext(this, v))
                .collect(Collectors.toList());

        this.declaredFields = Arrays.stream(rawType.getDeclaredFields())
                .map(v -> new FieldContext(this, v))
                .collect(Collectors.toList());

        this.declaredMethods = Arrays.stream(rawType.getDeclaredMethods())
                .map(v -> new MethodContext(this, v))
                .collect(Collectors.toList());

        final Map<String, FieldContext> allFields = new HashMap<>();
        final Map<Signature, MethodContext> allMethods = new HashMap<>();

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

        final Map<String, FieldContext> matchedFields = allFields.values().stream()
                .filter(HasModifiers.matchNot(Modifier.STATIC))
                .collect(Collectors.toMap(FieldContext::name, v -> v));

        final Map<String, MethodContext> matchedGetters = allMethods.values().stream()
                .filter(HasName.match(Pattern.compile("get[A-Z].*")))
                .filter(HasParameters.match(0))
                .filter(HasType.match(void.class).negate())
                .filter(HasModifiers.matchNot(Modifier.STATIC))
                .collect(Collectors.toMap(v -> lowerCamel(v.name().substring(3)), v -> v));

        final Map<String, MethodContext> matchedSetters = allMethods.values().stream()
                .filter(HasName.match(Pattern.compile("set[A-Z].*")))
                .filter(HasParameters.match(1))
                .filter(HasModifiers.matchNot(Modifier.STATIC))
                .collect(Collectors.toMap(v -> lowerCamel(v.name().substring(3)), v -> v));

        this.properties = Stream.<Map<String, ? extends HasName>>of(matchedFields, matchedGetters, matchedSetters)
                .map(Map::keySet).flatMap(Collection::stream)
                .distinct().map(v -> {
                    final FieldContext field = matchedFields.get(v);
                    final MethodContext getter = matchedGetters.get(v);
                    final MethodContext setter = matchedSetters.get(v);
                    return new PropertyContext(this, v,field, getter, setter);
                })
                .collect(Collectors.toList());

        this.typeParameters = Arrays.stream(rawType.getTypeParameters())
                    .map(v -> new TypeVariableContext(v, GenericTypeReflector.getTypeParameter(type, v)))
                    .collect(Collectors.toList());

        this.annotations = AnnotationContext.from(annotatedType);
    }

    public static <T> TypeContext from(final Class<T> type) {

        return from(GenericTypeReflector.annotate(type));
    }

    public static TypeContext from(final Type type) {

        return from(GenericTypeReflector.annotate(type));
    }

    @SuppressWarnings("unchecked")
    public static TypeContext from(final AnnotatedType type) {

        synchronized (CACHE) {
            final TypeContext result;
            if(!CACHE.containsKey(type)) {
                result = new TypeContext(type);
                CACHE.put(type, result);
            } else {
                result = CACHE.get(type);
            }
            return result;
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
    public TypeContext type() {

        return this;
    }

    public boolean isEnum() {

        return rawType.isEnum();
    }

    @SuppressWarnings("unchecked")
    public <T> T[] enumConstants() {

        return (T[])rawType.getEnumConstants();
    }

    public TypeContext find(final Class<?> type) {

        if(rawType == type) {
            return this;
        } else if(superclass != null) {
            final TypeContext found = superclass.find(type);
            if(found != null) {
                return found;
            }
        }
        for(final TypeContext iface : interfaces) {
            final TypeContext found = iface.find(type);
            if(found != null) {
                return found;
            }
        }
        return null;
    }

    @Data
    private static class Signature {

        private final String name;

        private final List<Class<?>> args;

        public static <T> Signature of(final MethodContext m) {

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

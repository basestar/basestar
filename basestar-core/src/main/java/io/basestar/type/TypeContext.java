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
import io.basestar.type.has.*;
import io.basestar.util.Nullsafe;
import io.basestar.util.Text;
import io.leangen.geantyref.GenericTypeReflector;
import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;
import lombok.experimental.Accessors;

import java.lang.reflect.AnnotatedParameterizedType;
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
@SuppressWarnings("Guava")
public class TypeContext implements HasName, HasModifiers, HasAnnotations, HasTypeParameters, HasType, HasConstructors,
        HasMethods, HasFields, HasProperties {

    private static final ConcurrentMap<AnnotatedType, TypeContext> CACHE = new ConcurrentHashMap<>();

    private final AnnotatedType annotatedType;

    private final Class<?> erasedType;

    private final Supplier<TypeContext> superclass;

    private final Supplier<List<TypeContext>> interfaces;

    private final Supplier<List<ConstructorContext>> declaredConstructors;

    private final Supplier<List<FieldContext>> declaredFields;

    private final Supplier<List<MethodContext>> declaredMethods;

    @Getter(AccessLevel.NONE)
    private final Supplier<Map<String, FieldContext>> allFields;

    @Getter(AccessLevel.NONE)
    private final Supplier<Map<Signature, MethodContext>> allMethods;

    private final Supplier<List<ConstructorContext>> constructors;

    private final Supplier<List<FieldContext>> fields;

    private final Supplier<List<MethodContext>> methods;

    private final Supplier<List<PropertyContext>> properties;

    private final Supplier<List<TypeVariableContext>> typeParameters;

    private final Supplier<List<AnnotationContext<?>>> annotations;

    private TypeContext(final AnnotatedType type) {

        this.annotatedType = type;
        this.erasedType = GenericTypeReflector.erase(type.getType());

        this.superclass = Suppliers.memoize(() -> {

            final Class<?> sc = erasedType.getSuperclass();
            return sc == null ? null : TypeContext.from(GenericTypeReflector.getExactSuperType(type, sc));
        });

        this.interfaces = Suppliers.memoize(() -> Arrays.stream(erasedType.getInterfaces())
                .map(ifc -> TypeContext.from(GenericTypeReflector.getExactSuperType(type, ifc)))
                .collect(Collectors.toList()));

        this.declaredConstructors = Suppliers.memoize(() -> Arrays.stream(erasedType.getDeclaredConstructors())
                .map(v -> new ConstructorContext(this, v))
                .collect(Collectors.toList()));

        this.declaredFields = Suppliers.memoize(() -> Arrays.stream(erasedType.getDeclaredFields())
                .map(v -> new FieldContext(this, v))
                .collect(Collectors.toList()));

        this.declaredMethods = Suppliers.memoize(() -> Arrays.stream(erasedType.getDeclaredMethods())
                .map(v -> new MethodContext(this, v))
                .collect(Collectors.toList()));

        this.constructors = Suppliers.memoize(() -> declaredConstructors().stream()
                .filter(HasModifiers.match(Modifier.PUBLIC))
                .collect(Collectors.toList()));

        this.allFields = Suppliers.memoize(() -> {

            final Map<String, FieldContext> allFields = new HashMap<>();

            final TypeContext sc = superclass();
            if (sc != null) {
                sc.fields().forEach(f -> allFields.put(f.name(), f));
            }
            interfaces().forEach(ifc -> {
                ifc.fields().forEach(f -> allFields.put(f.name(), f));
            });
            declaredFields().forEach(f -> allFields.put(f.name(), f));

            return allFields;
        });

        this.fields = Suppliers.memoize(() -> allFields.get().values().stream()
                .filter(HasModifiers.match(Modifier.PUBLIC))
                .collect(Collectors.toList()));

        this.allMethods = Suppliers.memoize(() -> {

            final Map<Signature, MethodContext> allMethods = new HashMap<>();

            final TypeContext sc = superclass();
            if (sc != null) {
                sc.methods().forEach(m -> allMethods.put(Signature.of(m), m));
            }
            interfaces().forEach(ifc -> {
                ifc.methods().forEach(m -> allMethods.put(Signature.of(m), m));
            });
            declaredMethods().forEach(m -> allMethods.put(Signature.of(m), m));

            return allMethods;
        });

        this.methods = Suppliers.memoize(() -> allMethods.get().values().stream()
                .filter(HasModifiers.match(Modifier.PUBLIC))
                .collect(Collectors.toList()));

        this.properties = Suppliers.memoize(() -> {

            final Map<String, FieldContext> allFields = this.allFields.get();
            final Map<Signature, MethodContext> allMethods = this.allMethods.get();

            final Map<String, FieldContext> matchedFields = allFields.values().stream()
                    .filter(HasModifiers.matchNot(Modifier.STATIC))
                    .collect(Collectors.toMap(FieldContext::name, v -> v));

            final Map<String, MethodContext> matchedGetters = allMethods.values().stream()
                    .filter(HasName.match("getClass").negate())
                    .filter(HasName.match(Pattern.compile("get[A-Z].*")))
                    .filter(HasParameters.match(0))
                    .filter(HasType.match(void.class).negate())
                    .filter(HasModifiers.matchNot(Modifier.STATIC))
                    .collect(Collectors.toMap(v -> Text.lowerCamel(v.name().substring(3)), v -> v));

            final Map<String, MethodContext> matchedSetters = allMethods.values().stream()
                    .filter(HasName.match(Pattern.compile("set[A-Z].*")))
                    .filter(HasParameters.match(1))
                    .filter(HasModifiers.matchNot(Modifier.STATIC))
                    .collect(Collectors.toMap(v -> Text.lowerCamel(v.name().substring(3)), v -> v));

            return Stream.<Map<String, ? extends HasName>>of(matchedFields, matchedGetters, matchedSetters)
                    .map(Map::keySet).flatMap(Collection::stream)
                    .distinct().map(v -> {
                        final FieldContext field = matchedFields.get(v);
                        final MethodContext getter = matchedGetters.get(v);
                        final MethodContext setter = matchedSetters.get(v);
                        return new PropertyContext(this, v, field, getter, setter);
                    })
                    .collect(Collectors.toList());
        });

        this.typeParameters = Suppliers.memoize(() -> Arrays.stream(erasedType.getTypeParameters())
                .map(v -> new TypeVariableContext(v, GenericTypeReflector.getTypeParameter(type, v)))
                .collect(Collectors.toList()));

        this.annotations = Suppliers.memoize(() -> AnnotationContext.from(annotatedType));
    }

    public static TypeContext from(final Type type, final Type... args) {

        return from(
                (AnnotatedParameterizedType) GenericTypeReflector.annotate(type),
                Arrays.stream(args).map(GenericTypeReflector::annotate).toArray(AnnotatedType[]::new)
        );
    }

    public static TypeContext from(final AnnotatedParameterizedType type, final AnnotatedType... args) {

        return from(GenericTypeReflector.replaceParameters(
                type,
                args
        ));
    }

    public static <T> TypeContext from(final Class<T> type) {

        return from(GenericTypeReflector.annotate(type));
    }

    public static TypeContext from(final Type type) {

        return from(GenericTypeReflector.annotate(type));
    }

    public static TypeContext from(final AnnotatedType type) {

        return CACHE.computeIfAbsent(type, TypeContext::new);
    }

    public TypeContext superclass() {

        return superclass.get();
    }

    public List<TypeContext> interfaces() {

        return interfaces.get();
    }

    @Override
    public List<ConstructorContext> declaredConstructors() {

        return declaredConstructors.get();
    }

    @Override
    public List<FieldContext> declaredFields() {

        return declaredFields.get();
    }

    @Override
    public List<MethodContext> declaredMethods() {

        return declaredMethods.get();
    }

    @Override
    public List<ConstructorContext> constructors() {

        return constructors.get();
    }

    @Override
    public List<FieldContext> fields() {

        return fields.get();
    }

    @Override
    public List<MethodContext> methods() {

        return methods.get();
    }

    public List<PropertyContext> properties() {

        return properties.get();
    }

    @Override
    public List<TypeVariableContext> typeParameters() {

        return typeParameters.get();
    }

    public TypeVariableContext typeParameter(final Class<?> type, final int offset) {

        return find(type).typeParameters().get(offset);
    }

    @Override
    public List<AnnotationContext<?>> annotations() {

        return annotations.get();
    }

    public TypeContext enclosing() {

        return TypeContext.from(erasedType.getEnclosingClass());
    }

    @Override
    public int modifiers() {

        return erasedType.getModifiers();
    }

    @Override
    public String name() {

        return erasedType.getName();
    }

    @Override
    public String simpleName() {

        return erasedType.getSimpleName();
    }

    @Override
    public TypeContext type() {

        return this;
    }

    public boolean isEnum() {

        return erasedType.isEnum();
    }

    public boolean isPrimitive() {

        return erasedType.isPrimitive();
    }

    public boolean isArray() {

        return erasedType.isArray();
    }

    public TypeContext arrayComponentType() {

        // FIXME: TypeContext (on class) annotations break unless we unwrap the annotated type here
        return TypeContext.from(GenericTypeReflector.getArrayComponentType(annotatedType.getType()));
    }

    public TypeContext box() {

        return TypeContext.from(GenericTypeReflector.box(erasedType));
    }

    public boolean isAssignableFrom(final TypeContext other) {

        return erasedType.isAssignableFrom(other.erasedType);
    }

    public boolean isAssignableFrom(final Class<?> other) {

        return erasedType.isAssignableFrom(other);
    }

    public boolean isAssignableTo(final TypeContext other) {

        return other.erasedType.isAssignableFrom(erasedType);
    }

    public boolean isAssignableTo(final Class<?> other) {

        return other.isAssignableFrom(erasedType);
    }

    @SuppressWarnings("unchecked")
    public <V> Class<V> erasedType() {

        return (Class<V>) erasedType;
    }

    @SuppressWarnings("unchecked")
    public <T extends Enum<?>> T[] enumConstants() {

        return (T[]) erasedType.getEnumConstants();
    }

    public TypeContext find(final Class<?> type) {

        final TypeContext superclass = this.superclass();
        final List<TypeContext> interfaces = this.interfaces();

        if (erasedType == type) {
            return this;
        } else if (superclass != null) {
            final TypeContext found = superclass.find(type);
            if (found != null) {
                return found;
            }
        }
        for (final TypeContext iface : interfaces) {
            final TypeContext found = iface.find(type);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    public String packageName() {

        return Nullsafe.map(erasedType.getPackage(), Package::getName);
    }

    @Data
    private static class Signature {

        private final String name;

        private final List<Class<?>> args;

        public static <T> Signature of(final MethodContext m) {

            return new Signature(m.name(), m.erasedParameterTypes());
        }
    }
}

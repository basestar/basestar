package io.basestar.mapper;

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

import io.basestar.mapper.internal.TypeMapper;
import io.basestar.mapper.internal.annotation.SchemaDeclaration;
import io.basestar.mapper.internal.annotation.SchemaModifier;
import io.basestar.schema.Namespace;
import io.basestar.schema.Schema;
import io.basestar.type.AnnotationContext;
import io.basestar.type.PropertyContext;
import io.basestar.type.TypeContext;
import io.basestar.type.has.HasType;
import io.basestar.util.Name;
import lombok.RequiredArgsConstructor;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.stream.Collectors;

@RequiredArgsConstructor
public class MappingContext implements Serializable {

    private final MappingStrategy strategy;

    private final Map<Class<?>, SchemaMapper<?, ?>> mappers = new HashMap<>();

    public MappingContext() {

        this(MappingStrategy.DEFAULT);
    }

    public MappingStrategy strategy() {

        return strategy;
    }

    public Namespace.Builder namespace(final Class<?> ... classes) {

        return namespace(Arrays.asList(classes));
    }

    public Namespace.Builder namespace(final Collection<Class<?>> classes) {

        final Set<Class<?>> dependencies = new HashSet<>(classes);
        final Map<Class<?>, SchemaMapper<?, ?>> all = new HashMap<>();
        while(!dependencies.isEmpty()) {
            final Set<Class<?>> next = new HashSet<>();
            dependencies.forEach(cls -> {
                if(!all.containsKey(cls)) {
                    final SchemaMapper<?, ?> schemaMapper = schemaMapper(cls);
                    all.put(cls, schemaMapper);
                    next.addAll(schemaMapper.dependencies());
                }
            });
            dependencies.clear();
            dependencies.addAll(next);
        }
        final Namespace.Builder builder = Namespace.builder();
        all.forEach((cls, schemaMapper) -> builder.setSchema(schemaMapper.name(), schemaMapper.schemaBuilder()));
        return builder;
    }

    public boolean isSchema(final Class<?> cls) {

        final SchemaMapper<?, ?> mapper = this.mappers.get(cls);
        if(mapper != null) {
            return true;
        } else {
            final TypeContext type = TypeContext.from(cls);
            return !declarationAnnotations(type).isEmpty();
        }
    }

    public Name schemaName(final Class<?> cls) {

        final SchemaMapper<?, ?>  mapper = this.mappers.get(cls);
        if(mapper != null) {
            return mapper.qualifiedName();
        } else {
            final TypeContext type = TypeContext.from(cls);
            final SchemaDeclaration.Declaration decl = declaration(type);
            return decl.getQualifiedName(this, type);
        }
    }

    public Schema<?> schema(final Class<?> cls) {

        final Name name = schemaName(cls);
        return namespace(cls).build().requireSchema(name);
    }
    
    public Schema<?> schema(final Schema.Resolver resolver, final Class<?> cls) {

        final Name name = schemaName(cls);
        return namespace(cls).build(resolver).requireSchema(name);
    }

    public TypeMapper typeMapper(final PropertyContext property) {

        final TypeContext type = property.type();
        if(strategy.isOptional(property)) {
            return new TypeMapper.OfOptional(type.erasedType(), typeMapper(type));
        } else {
            return typeMapper(type);
        }
    }

    public TypeMapper typeMapper(final TypeContext type) {

        return strategy.typeMapper(this, type);
    }

    @SuppressWarnings("unchecked")
    public <T, O> SchemaMapper<T, O> schemaMapper(final Class<T> cls) {

        return (SchemaMapper<T, O>) this.mappers.computeIfAbsent(cls, this::newSchemaMapper);
    }

    @SuppressWarnings("unchecked")
    private <T, O> SchemaMapper<T, O> newSchemaMapper(final Class<T> cls) {

        final TypeContext type = TypeContext.from(cls);
        final SchemaDeclaration.Declaration decl = declaration(type);
        final SchemaMapper.Builder<T, O> builder = (SchemaMapper.Builder<T, O>)decl.mapper(this, type);
        applyModifiers(type, builder);
        return builder.build();
    }

    private List<AnnotationContext<?>> declarationAnnotations(final TypeContext type) {

        return type.annotations().stream()
                .filter(a -> a.type().annotations().stream()
                        .anyMatch(HasType.match(SchemaDeclaration.class)))
                .collect(Collectors.toList());
    }

    private SchemaDeclaration.Declaration declaration(final TypeContext type) {

        final List<AnnotationContext<?>> declAnnotations = declarationAnnotations(type);

        if (declAnnotations.size() == 0) {
            return SchemaDeclaration.Declaration.Basic.INSTANCE;
        } else if (declAnnotations.size() == 1) {
            try {
                final AnnotationContext<?> annotation = declAnnotations.get(0);
                final SchemaDeclaration schemaDeclaration = annotation.type().annotation(SchemaDeclaration.class).annotation();
                final TypeContext declType = TypeContext.from(schemaDeclaration.value());
                return declType.declaredConstructors().get(0).newInstance(annotation.annotation());
            } catch (final IllegalAccessException | InstantiationException | InvocationTargetException e) {
                throw new IllegalStateException(e);
            }
        } else {
            final String names = declAnnotations.stream().map(v -> v.type().simpleName())
                    .collect(Collectors.joining(", "));
            throw new IllegalStateException("Annotations " + names + " are not allowed on the same type");
        }
    }

    private <T, O> void applyModifiers(final TypeContext type, final SchemaMapper.Builder<T, O> input) {

        final List<AnnotationContext<?>> modAnnotations = type.annotations().stream()
                .filter(a -> a.type().annotations().stream()
                        .anyMatch(HasType.match(SchemaModifier.class)))
                .collect(Collectors.toList());

        for(final AnnotationContext<?> annotation : modAnnotations) {
            try {
                final SchemaModifier schemaModifier = annotation.type().annotation(SchemaModifier.class).annotation();
                final TypeContext modType = TypeContext.from(schemaModifier.value());
                final Class<?> modMapperType = modType.find(SchemaModifier.Modifier.class).typeParameters().get(0).type().erasedType();
                if(modMapperType.isAssignableFrom(input.getClass())) {
                    final SchemaModifier.Modifier<SchemaMapper.Builder<T, O>> mod = modType.declaredConstructors().get(0).newInstance(annotation.annotation());
                    mod.modify(this, input);
                } else {
                    throw new IllegalStateException("Modifier " + modType.erasedType() + " not supported on " + input.getClass());
                }
            } catch (final IllegalAccessException | InstantiationException | InvocationTargetException e) {
                throw new IllegalStateException(e);
            }
        }
    }
}

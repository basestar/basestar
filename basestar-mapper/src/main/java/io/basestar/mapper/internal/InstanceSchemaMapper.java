package io.basestar.mapper.internal;

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
import io.basestar.mapper.MappingContext;
import io.basestar.mapper.SchemaMapper;
import io.basestar.mapper.internal.annotation.MemberDeclaration;
import io.basestar.mapper.internal.annotation.MemberModifier;
import io.basestar.schema.*;
import io.basestar.type.AnnotationContext;
import io.basestar.type.PropertyContext;
import io.basestar.type.TypeContext;
import io.basestar.type.has.HasType;
import io.basestar.util.Name;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.stream.Collectors;

// FIXME: inheritance not implemented

public abstract class InstanceSchemaMapper<T, B extends InstanceSchema.Builder> implements SchemaMapper<T, Map<String, Object>> {

    protected final Name name;

    protected final Name extend;

    protected final boolean concrete;

    protected final Class<T> erasedType;

    protected final String description;

    private final String packageName;

    protected final List<MemberMapper<B>> members;

    @SuppressWarnings("unchecked")
    protected InstanceSchemaMapper(final Class<B> builderType, final MappingContext context, final Name name, final TypeContext type) {

        this.name = name;
        this.extend = context.strategy().extend(type).map(e -> context.schemaName(e.erasedType())).orElse(null);
        this.concrete = context.strategy().concrete(type);
        this.erasedType = type.erasedType();
        this.description = null;
        this.packageName = type.packageName();

        // Filter out properties that appear in base types
        final Set<String> skipNames = context.strategy().extend(type)
                .map(extend -> extend.properties().stream().map(PropertyContext::name).collect(Collectors.toSet()))
                .orElse(Collections.emptySet());

        final List<MemberMapper<B>> members = new ArrayList<>();
        type.properties().forEach(prop -> {

            if(!skipNames.contains(prop.name())) {

                try {
                    final List<AnnotationContext<?>> propAnnotations = prop.annotations().stream()
                            .filter(a -> a.type().annotations().stream()
                                    .anyMatch(HasType.match(MemberDeclaration.class)))
                            .collect(Collectors.toList());

                    if (propAnnotations.size() == 0) {
                        // FIXME
                        members.add((MemberMapper<B>) new PropertyMapper(context, prop.name(), prop));
                    } else if (propAnnotations.size() == 1) {
                        final AnnotationContext<?> annotation = propAnnotations.get(0);
                        final MemberDeclaration memberDeclaration = annotation.type().annotation(MemberDeclaration.class).annotation();
                        final TypeContext declType = TypeContext.from(memberDeclaration.value());
                        final MemberDeclaration.Declaration decl = member(declType, annotation.annotation());
                        final MemberMapper<?> member = decl.mapper(context, prop);
                        final TypeContext mapperType = TypeContext.from(member.getClass());
                        final Class<?> memberBuilderType = mapperType.find(MemberMapper.class).typeParameters().get(0).type().erasedType();
                        if (memberBuilderType.isAssignableFrom(builderType)) {
                            members.add(applyModifiers(context, prop, (MemberMapper<B>) member));
                        } else {
                            throw new IllegalStateException("Member " + member.getClass() + " not supported on " + this.getClass());
                        }
                    } else {
                        final String names = propAnnotations.stream().map(v -> v.type().simpleName())
                                .collect(Collectors.joining(", "));
                        throw new IllegalStateException("Annotations " + names + " are not allowed on the same property");
                    }

                } catch (final Exception e) {
                    throw new IllegalStateException("Failed to map property", e);
                }
            }

        });

        this.members = members;
    }

    protected InstanceSchemaMapper(final InstanceSchemaMapper<T, B> copy, final String description) {

        this.name = copy.name;
        this.concrete = copy.concrete;
        this.extend = copy.extend;
        this.erasedType = copy.erasedType;
        this.members = copy.members;
        this.description = description;
        this.packageName = copy.packageName;
    }

    private MemberMapper<B> applyModifiers(final MappingContext context, final PropertyContext prop, final MemberMapper<B> input) throws IllegalAccessException, InstantiationException, InvocationTargetException {

        final List<AnnotationContext<?>> modAnnotations = prop.annotations().stream()
                .filter(a -> a.type().annotations().stream()
                        .anyMatch(HasType.match(MemberModifier.class)))
                .collect(Collectors.toList());

        MemberMapper<B> output = input;

        for(final AnnotationContext<?> annotation : modAnnotations) {
            final MemberModifier memberModifier = annotation.type().annotation(MemberModifier.class).annotation();
            final TypeContext modType = TypeContext.from(memberModifier.value());
            final Class<?> modMapperType = modType.find(MemberModifier.Modifier.class).typeParameters().get(0).type().erasedType();
            if(modMapperType.isAssignableFrom(output.getClass())) {
                final MemberModifier.Modifier<MemberMapper<B>> mod = modType.declaredConstructors().get(0).newInstance(annotation.annotation());
                output = mod.modify(context, output);
            } else {
                throw new IllegalStateException("Modifier " + modType.erasedType() + " not supported on " + output.getClass());
            }
        }

        return output;
    }

    @Override
    public abstract B schemaBuilder();

    protected B addMembers(final B builder) {

        members.forEach(m -> m.addToSchema(this, builder));
        builder.setDescription(description);
        builder.setExtensions(ImmutableMap.of(
                "io.basestar.package", packageName
        ));
        return builder;
    }

    protected void addProperty(final B builder, final String name, final Property.Builder property) {

        builder.setProperty(name, property);
    }

    protected void addLink(final B builder, final String name, final Link.Builder link) {

        if(builder instanceof Link.Resolver.Builder) {
            ((Link.Resolver.Builder)builder).setLink(name, link);
        } else {
            throw new IllegalStateException("Cannot add link to " + builder.getType());
        }
    }

    protected void addTransient(final B builder, final String name, final Transient.Builder trans) {

        if(builder instanceof Transient.Resolver.Builder) {
            ((Transient.Resolver.Builder)builder).setTransient(name, trans);
        } else {
            throw new IllegalStateException("Cannot add link to " + builder.getType());
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public Class<Map<String, Object>> unmarshalledType() {

        return (Class<Map<String, Object>>)(Class<?>)Map.class;
    }

    @Override
    public Class<T> marshalledType() {

        return erasedType;
    }

    @Override
    public Name qualifiedName() {

        return name;
    }

    @Override
    @SuppressWarnings("unchecked")
    public T marshall(final Object source) {

        if(source == null) {
            return null;
        } else if(source instanceof Map<?, ?>) {
            final Map<String, Object> value = (Map<String, Object>)source;
            try {
                final T target = erasedType.newInstance();
                for (final MemberMapper<B> member : members) {
                    member.marshall(value, target);
                }
                return target;
            } catch (final InstantiationException | InvocationTargetException | IllegalAccessException e) {
                throw new IllegalStateException(e);
            }
        } else {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public Map<String, Object> unmarshall(final T source) {

        if(source == null) {
            return null;
        } else {
            try {
                final Map<String, Object> target = new HashMap<>();
                for (final MemberMapper<B> member : members) {
                    member.unmarshall(source, target);
                }
                target.put(ObjectSchema.SCHEMA, name.toString());
                return target;
            } catch (final InvocationTargetException | IllegalAccessException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    @Override
    public Set<Class<?>> dependencies() {

        final Set<Class<?>> all = new HashSet<>();
        members.forEach(m -> all.addAll(m.dependencies()));
        return all;
    }

    private static MemberDeclaration.Declaration member(final TypeContext declType, final Annotation annotation) {

        try {
            // FIXME:
            return declType.declaredConstructors().get(0).newInstance(annotation);
        } catch (final IllegalAccessException | InstantiationException | InvocationTargetException e) {
            throw new IllegalStateException(e);
        }
    }
}

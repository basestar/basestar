package io.basestar.mapper.internal;

import io.basestar.mapper.MappingContext;
import io.basestar.mapper.SchemaMapper;
import io.basestar.mapper.internal.annotation.MemberDeclaration;
import io.basestar.schema.InstanceSchema;
import io.basestar.type.AnnotationContext;
import io.basestar.type.TypeContext;
import io.basestar.type.has.HasType;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.stream.Collectors;

// FIXME: inheritance not implemented

public abstract class InstanceSchemaMapper<T, B extends InstanceSchema.Builder> implements SchemaMapper<T, Map<String, Object>> {

    private final String name;

    private final TypeContext type;

    private final List<MemberMapper<B>> members;

    @SuppressWarnings("unchecked")
    public InstanceSchemaMapper(final MappingContext context, final String name, final TypeContext type, final Class<B> builderType) {

        this.name = name;
        this.type = type;

//        TypeContext superclass = type.superclass();
//        while(!superclass.erasedType().equals(Object.class)) {
//            if()
//            superclass = superclass.superclass();
//        }

        final List<MemberMapper<B>> members = new ArrayList<>();
        type.properties().forEach(prop -> {

            try {
                final List<AnnotationContext<?>> propAnnotations = prop.annotations().stream()
                        .filter(a -> a.type().annotations().stream()
                                .anyMatch(HasType.match(MemberDeclaration.class)))
                        .collect(Collectors.toList());

                if (propAnnotations.size() == 0) {
                    // FIXME
                    members.add((MemberMapper<B>)new PropertyMapper(context, prop.name(), prop));
                } else if (propAnnotations.size() == 1) {
                    final AnnotationContext<?> annotation = propAnnotations.get(0);
                    final MemberDeclaration memberDeclaration = annotation.type().annotation(MemberDeclaration.class).annotation();
                    final TypeContext declType = TypeContext.from(memberDeclaration.value());
                    final MemberDeclaration.Declaration decl = member(declType, annotation.annotation());
                    final MemberMapper<?> member = decl.mapper(context, prop);
                    final TypeContext mapperType = TypeContext.from(member.getClass());
                    final Class<?> memberBuilderType = mapperType.find(MemberMapper.class).typeParameters().get(0).type().erasedType();
                    if(memberBuilderType.isAssignableFrom(builderType)) {
                        members.add((MemberMapper<B>)member);
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
        });

        this.members = members;
    }

    @Override
    public abstract B schema();

    protected B addMembers(final B builder) {

        members.forEach(m -> m.addToSchema(builder));
        return builder;
    }

    @Override
    public String name() {

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
                final T target = (T) type.erasedType().newInstance();
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

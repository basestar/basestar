package io.basestar.mapper.internal;

import io.basestar.mapper.internal.annotation.SchemaDeclaration;
import io.basestar.schema.Schema;
import io.basestar.type.AnnotationContext;
import io.basestar.type.TypeContext;
import io.basestar.type.has.HasType;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.stream.Collectors;

public interface SchemaMapper<T, O> {

    String name();

    Schema.Builder<? extends O> schema();

    T marshall(Object value);

    O unmarshall(T value);

    static <T, O> SchemaMapper<T, O> mapper(final Class<T> cls) {

        return mapper(TypeContext.from(cls));
    }

    @SuppressWarnings("unchecked")
    static <T, O> SchemaMapper<T, O> mapper(final TypeContext type) {

        final List<AnnotationContext<?>> schemaAnnotations = type.annotations().stream()
                .filter(a -> a.type().annotations().stream()
                        .anyMatch(HasType.match(SchemaDeclaration.class)))
                .collect(Collectors.toList());

        if (schemaAnnotations.size() == 0) {
            final String name = type.simpleName();
            if (type.isEnum()) {
                return (SchemaMapper<T, O>) new EnumSchemaMapper<>(name, type);
            } else {
                return (SchemaMapper<T, O>) new StructSchemaMapper<>(name, type);
            }
        } else if (schemaAnnotations.size() == 1) {
            try {
                final AnnotationContext<?> annotation = schemaAnnotations.get(0);
                final SchemaDeclaration schemaDeclaration = annotation.type().annotation(SchemaDeclaration.class).annotation();
                final TypeContext declType = TypeContext.from(schemaDeclaration.value());
                final SchemaDeclaration.Declaration decl = declType.declaredConstructors().get(0).newInstance(annotation.annotation());
                return (SchemaMapper<T, O>) decl.mapper(type);
            } catch (final IllegalAccessException | InstantiationException | InvocationTargetException e) {
                throw new IllegalStateException(e);
            }
        } else {
            final String names = schemaAnnotations.stream().map(v -> v.type().simpleName())
                    .collect(Collectors.joining(", "));
            throw new IllegalStateException("Annotations " + names + " are not allowed on the same type");
        }
    }
}

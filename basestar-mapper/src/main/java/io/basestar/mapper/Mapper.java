package io.basestar.mapper;

import io.basestar.mapper.annotation.*;
import io.basestar.mapper.internal.PropertyBinder;
import io.basestar.mapper.internal.SchemaBinder;
import io.basestar.mapper.internal.annotation.PropertyAnnotation;
import io.basestar.mapper.internal.annotation.SchemaAnnotation;
import io.basestar.mapper.type.HasType;
import io.basestar.mapper.type.WithAnnotation;
import io.basestar.mapper.type.WithType;
import io.basestar.schema.Schema;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.stream.Collectors;

//import io.basestar.schema.Link;
//import io.basestar.schema.Schema;

public class Mapper {

//    private final Map<Class<?>, Schema<?>> schemas = new IdentityHashMap<>();

    private static SchemaBinder schemaBinder(final WithType<? extends SchemaBinder> binderType, final Annotation annotation) {

        try {
            // FIXME:
            return binderType.declaredConstructors().get(0).newInstance(annotation);
        } catch (final IllegalAccessException | InstantiationException | InvocationTargetException e) {
            throw new IllegalStateException(e);
        }
    }

    private static PropertyBinder propertyBinder(final WithType<? extends PropertyBinder> binderType, final Annotation annotation) {

        try {
            // FIXME:
            return binderType.declaredConstructors().get(0).newInstance(annotation);
        } catch (final IllegalAccessException | InstantiationException | InvocationTargetException e) {
            throw new IllegalStateException(e);
        }
    }

    public <T> Schema<?> schema(final Class<T> cls) {

        final WithType<T> with = WithType.with(cls);

        final List<WithAnnotation<?>> schemaAnnotations = with.annotations().stream()
                .filter(a -> a.type().annotations().stream()
                        .anyMatch(HasType.match(SchemaAnnotation.class)))
                .collect(Collectors.toList());

        if (schemaAnnotations.size() == 0) {

            if(with.isEnum()) {

            }

        } else if (schemaAnnotations.size() == 1) {
            final WithAnnotation<?> annotation = schemaAnnotations.get(0);
            final SchemaAnnotation schemaAnnotation = annotation.type().annotation(SchemaAnnotation.class).annotation();
            final WithType<? extends SchemaBinder> binderType = WithType.with(schemaAnnotation.value());
            final SchemaBinder baseBinder = schemaBinder(binderType, annotation.annotation());
//
//            return bindSchema(with, baseBinder);
//
//            final Schema.Builder<?> builder = binder.schemaBuilder(with);
//            System.err.println(builder);

        } else {
            final String names = schemaAnnotations.stream().map(v -> v.type().simpleName())
                    .collect(Collectors.joining(", "));
            throw new IllegalStateException("Annotations " + names + " are not allowed on the same type");
        }
//        final Map<String, >

        with.properties().stream()
                .map(prop -> {

            try {

                final List<WithAnnotation<?>> propAnnotations = prop.annotations().stream()
                        .filter(a -> a.type().annotations().stream()
                                .anyMatch(HasType.match(PropertyAnnotation.class)))
                        .collect(Collectors.toList());

                if (propAnnotations.size() == 0) {

                } else if (propAnnotations.size() == 1) {
                    final WithAnnotation<?> annotation = propAnnotations.get(0);
                    final PropertyAnnotation propertyAnnotation = annotation.type().annotation(PropertyAnnotation.class).annotation();
                    final WithType<? extends PropertyBinder> binderType = WithType.with(propertyAnnotation.value());
                    final PropertyBinder binder = propertyBinder(binderType, annotation.annotation());

//
//                    final Class<? extends PropertyBinder> binderType = propertyAnnotation.value();
//                    binderType.getConstructor().newInstance();


                } else {
                    final String names = propAnnotations.stream().map(v -> v.type().simpleName())
                            .collect(Collectors.joining(", "));
                    throw new IllegalStateException("Annotations " + names + " are not allowed on the same property");
                }


                final WithAnnotation<Id> id = prop.annotation(Id.class);
                final WithAnnotation<Property> property = prop.annotation(Property.class);
                final WithAnnotation<Link> link = prop.annotation(Link.class);
                final WithAnnotation<Created> created = prop.annotation(Created.class);
                final WithAnnotation<Updated> updated = prop.annotation(Updated.class);
                final WithAnnotation<Version> version = prop.annotation(Version.class);
                final WithAnnotation<Hash> hash = prop.annotation(Hash.class);

            } catch (final Exception e) {
                throw new IllegalStateException("Failed to map property", e);
            }

            return null;

        });

//        with.properties().stream()
//                .filter(HasAnnotations.match(Property.class))
//                .collect(Collectors.toList());


        return null;

    }

    private <T> SchemaBinder bindNext(final WithType<T> with, final SchemaBinder baseBinder) {

        return new SchemaBinder() {

            @Override
            public String name(final WithType<?> type) {

                return baseBinder.name(type);
            }

            @Override
            public Schema.Builder schemaBuilder(final WithType<?> type) {

                final Schema.Builder<?> builder = baseBinder.schemaBuilder(type);

                return null;
            }
        };
    }

//    public <T> T marshall(final Class<T> cls, final Map<String, Object> instance) {
//
//    }
//
//    <T> Instance unmarshall(Class<T> cls, T instance);
}

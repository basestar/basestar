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

import io.basestar.mapper.context.AnnotationContext;
import io.basestar.mapper.context.TypeContext;
import io.basestar.mapper.context.has.HasType;
import io.basestar.mapper.internal.PropertyBinder;
import io.basestar.mapper.internal.SchemaBinder;
import io.basestar.mapper.internal.annotation.PropertyAnnotation;
import io.basestar.mapper.internal.annotation.SchemaAnnotation;
import io.basestar.schema.EnumSchema;
import io.basestar.schema.InstanceSchema;
import io.basestar.schema.Schema;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.stream.Collectors;

//import io.basestar.schema.Link;
//import io.basestar.schema.Schema;

public class Mapper {

//    private final Map<Class<?>, Schema<?>> schemas = new IdentityHashMap<>();

    private static SchemaBinder schemaBinder(final TypeContext binderType, final Annotation annotation) {

        try {
            // FIXME:
            return binderType.declaredConstructors().get(0).newInstance(annotation);
        } catch (final IllegalAccessException | InstantiationException | InvocationTargetException e) {
            throw new IllegalStateException(e);
        }
    }

    private static PropertyBinder propertyBinder(final TypeContext binderType, final Annotation annotation) {

        try {
            // FIXME:
            return binderType.declaredConstructors().get(0).newInstance(annotation);
        } catch (final IllegalAccessException | InstantiationException | InvocationTargetException e) {
            throw new IllegalStateException(e);
        }
    }

    public <T extends Schema<?>> T schema(final Class<?> cls) {

        final TypeContext with = TypeContext.from(cls);

        final List<AnnotationContext<?>> schemaAnnotations = with.annotations().stream()
                .filter(a -> a.type().annotations().stream()
                        .anyMatch(HasType.match(SchemaAnnotation.class)))
                .collect(Collectors.toList());

        final Schema.Builder<?> builder;
        if (schemaAnnotations.size() == 0) {
            if(with.isEnum()) {
                builder = EnumSchema.builder();
            } else {
                builder = null;
            }
        } else if (schemaAnnotations.size() == 1) {
            final AnnotationContext<?> annotation = schemaAnnotations.get(0);
            final SchemaAnnotation schemaAnnotation = annotation.type().annotation(SchemaAnnotation.class).annotation();
            final TypeContext binderType = TypeContext.from(schemaAnnotation.value());
            final SchemaBinder binder = schemaBinder(binderType, annotation.annotation());
            builder = binder.schemaBuilder(with);
        } else {
            final String names = schemaAnnotations.stream().map(v -> v.type().simpleName())
                    .collect(Collectors.joining(", "));
            throw new IllegalStateException("Annotations " + names + " are not allowed on the same type");
        }

        if(builder instanceof InstanceSchema.Builder) {

            with.properties().forEach(prop -> {

                try {

                    final List<AnnotationContext<?>> propAnnotations = prop.annotations().stream()
                            .filter(a -> a.type().annotations().stream()
                                    .anyMatch(HasType.match(PropertyAnnotation.class)))
                            .collect(Collectors.toList());

                    if (propAnnotations.size() == 0) {

                    } else if (propAnnotations.size() == 1) {
                        final AnnotationContext<?> annotation = propAnnotations.get(0);
                        final PropertyAnnotation propertyAnnotation = annotation.type().annotation(PropertyAnnotation.class).annotation();
                        final TypeContext binderType = TypeContext.from(propertyAnnotation.value());
                        final PropertyBinder binder = propertyBinder(binderType, annotation.annotation());

//
//                    final Class<? extends PropertyBinder> binderType = propertyAnnotation.value();
//                    binderType.getConstructor().newInstance();


                    } else {
                        final String names = propAnnotations.stream().map(v -> v.type().simpleName())
                                .collect(Collectors.joining(", "));
                        throw new IllegalStateException("Annotations " + names + " are not allowed on the same property");
                    }


                } catch (final Exception e) {
                    throw new IllegalStateException("Failed to map property", e);
                }

//            return null;

            });

//        with.properties().stream()
//                .filter(HasAnnotations.match(Property.class))
//                .collect(Collectors.toList());

        }

        return null;

    }

//    private <T> SchemaBinder bindNext(final TypeContext<T> with, final SchemaBinder baseBinder) {
//
//        return new SchemaBinder() {
//
//            @Override
//            public String name(final TypeContext<?> type) {
//
//                return baseBinder.name(type);
//            }
//
//            @Override
//            public Schema.Builder<?> schemaBuilder(final TypeContext<?> type) {
//
//                final Schema.Builder<?> builder = baseBinder.schemaBuilder(type);
//
//                return null;
//            }
//        };
//    }

//    public <T> T marshall(final Class<T> cls, final Map<String, Object> instance) {
//
//    }
//
//    <T> Instance unmarshall(Class<T> cls, T instance);
}

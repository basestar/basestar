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
import io.basestar.mapper.internal.annotation.BindNamespace;
import io.basestar.mapper.internal.annotation.BindSchema;
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

    private static BindNamespace.Handler schemaBinder(final TypeContext binderType, final Annotation annotation) {

        try {
            // FIXME:
            return binderType.declaredConstructors().get(0).newInstance(annotation);
        } catch (final IllegalAccessException | InstantiationException | InvocationTargetException e) {
            throw new IllegalStateException(e);
        }
    }

    private static BindSchema.Handler memberBinder(final TypeContext binderType, final Annotation annotation) {

        try {
            // FIXME:
            return binderType.declaredConstructors().get(0).newInstance(annotation);
        } catch (final IllegalAccessException | InstantiationException | InvocationTargetException e) {
            throw new IllegalStateException(e);
        }
    }

    @SuppressWarnings("unchecked")
    public <T extends Schema.Builder<?>> T schema(final Class<?> cls) {

        final TypeContext type = TypeContext.from(cls);

        final List<AnnotationContext<?>> schemaAnnotations = type.annotations().stream()
                .filter(a -> a.type().annotations().stream()
                        .anyMatch(HasType.match(BindNamespace.class)))
                .collect(Collectors.toList());

        final Schema.Builder<?> builder;
        if (schemaAnnotations.size() == 0) {
            if(type.isEnum()) {
                builder = EnumSchema.builder();
            } else {
                builder = null;
            }
        } else if (schemaAnnotations.size() == 1) {
            final AnnotationContext<?> annotation = schemaAnnotations.get(0);
            final BindNamespace bindNamespace = annotation.type().annotation(BindNamespace.class).annotation();
            final TypeContext binderType = TypeContext.from(bindNamespace.value());
            final BindNamespace.Handler binder = schemaBinder(binderType, annotation.annotation());
            builder = binder.schemaBuilder(type);
        } else {
            final String names = schemaAnnotations.stream().map(v -> v.type().simpleName())
                    .collect(Collectors.joining(", "));
            throw new IllegalStateException("Annotations " + names + " are not allowed on the same type");
        }

        if(builder instanceof InstanceSchema.Builder) {

            final InstanceSchema.Builder instanceBuilder = (InstanceSchema.Builder)builder;

            type.properties().forEach(prop -> {

                try {

                    final List<AnnotationContext<?>> propAnnotations = prop.annotations().stream()
                            .filter(a -> a.type().annotations().stream()
                                    .anyMatch(HasType.match(BindSchema.class)))
                            .collect(Collectors.toList());

                    if (propAnnotations.size() == 0) {

                    } else if (propAnnotations.size() == 1) {
                        final AnnotationContext<?> annotation = propAnnotations.get(0);
                        final BindSchema bindSchema = annotation.type().annotation(BindSchema.class).annotation();
                        final TypeContext binderType = TypeContext.from(bindSchema.value());
                        final BindSchema.Handler binder = memberBinder(binderType, annotation.annotation());
                        binder.addToSchema(instanceBuilder, prop);
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

        return (T)builder;

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

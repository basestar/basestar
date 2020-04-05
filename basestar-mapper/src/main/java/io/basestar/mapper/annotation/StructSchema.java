package io.basestar.mapper.annotation;

import io.basestar.mapper.internal.SchemaBinder;
import io.basestar.mapper.internal.annotation.SchemaAnnotation;
import io.basestar.mapper.type.WithType;
import io.basestar.schema.Schema;
import lombok.RequiredArgsConstructor;

import java.lang.annotation.*;

@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@SchemaAnnotation(StructSchema.Binder.class)
public @interface StructSchema {

    String INFER_NAME = "";

    String name() default INFER_NAME;

    @RequiredArgsConstructor
    class Binder implements SchemaBinder {

        private final StructSchema annotation;

        @Override
        public String name(final WithType<?> type) {

            final String name = annotation.name();
            return name.equals(INFER_NAME) ? type.simpleName() : name;
        }

        @Override
        public Schema.Builder<?> schemaBuilder(final WithType<?> type) {

            return io.basestar.schema.ObjectSchema.builder();
        }
    }
}

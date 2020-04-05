package io.basestar.mapper.annotation;

import io.basestar.mapper.internal.PropertyBinder;
import io.basestar.mapper.internal.annotation.PropertyAnnotation;
import io.basestar.mapper.type.WithProperty;
import io.basestar.schema.Reserved;
import lombok.RequiredArgsConstructor;

import java.lang.annotation.*;

@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD, ElementType.METHOD})
@PropertyAnnotation(Property.Binder.class)
public @interface Property {

    String INFER_NAME = "";

    String name() default INFER_NAME;

    boolean secret() default false;

    @RequiredArgsConstructor
    class Binder implements PropertyBinder {

        private final Property annotation;

        @Override
        public String name(final WithProperty<?, ?> property) {

            return Reserved.ID;
        }
    }
}

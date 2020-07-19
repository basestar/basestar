package io.basestar.mapper.annotation;

import com.google.common.collect.ImmutableMap;
import io.basestar.mapper.internal.PropertyMapper;
import io.basestar.mapper.internal.annotation.MemberModifier;
import io.basestar.type.AnnotationContext;
import lombok.RequiredArgsConstructor;

import java.lang.annotation.*;

@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD, ElementType.METHOD})
@MemberModifier(Required.Modifier.class)
public @interface Required {

    boolean value() default true;

    @RequiredArgsConstructor
    class Modifier implements MemberModifier.Modifier<PropertyMapper> {

        private final Required annotation;

        @Override
        public PropertyMapper modify(final PropertyMapper mapper) {

            return mapper.withRequired(annotation.value());
        }

        public static Required annotation(final boolean required) {

            return new AnnotationContext<>(Required.class, ImmutableMap.of("value", required)).annotation();
        }
    }
}
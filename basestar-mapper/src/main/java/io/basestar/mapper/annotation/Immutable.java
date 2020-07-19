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
@MemberModifier(Immutable.Modifier.class)
public @interface Immutable {

    boolean value() default true;

    @RequiredArgsConstructor
    class Modifier implements MemberModifier.Modifier<PropertyMapper> {

        private final Immutable annotation;

        @Override
        public PropertyMapper modify(final PropertyMapper mapper) {

            return mapper.withImmutable(annotation.value());
        }

        public static Immutable annotation(final boolean immutable) {

            return new AnnotationContext<>(Immutable.class, ImmutableMap.of("value", immutable)).annotation();
        }
    }
}
package io.basestar.mapper.annotation;

import io.basestar.mapper.internal.PropertyMapper;
import io.basestar.mapper.internal.annotation.MemberModifier;
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

        public static Immutable from(final boolean immutable) {

            return new Immutable() {

                @Override
                public Class<? extends Annotation> annotationType() {

                    return Immutable.class;
                }

                @Override
                public boolean value() {

                    return immutable;
                }
            };
        }
    }
}
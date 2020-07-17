package io.basestar.mapper.annotation;

import io.basestar.expression.Expression;
import io.basestar.mapper.internal.ViewSchemaMapper;
import io.basestar.mapper.internal.annotation.SchemaModifier;
import lombok.RequiredArgsConstructor;

import java.lang.annotation.*;

@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@SchemaModifier(Where.Modifier.class)
public @interface Where {

    String value();

    @RequiredArgsConstructor
    class Modifier implements SchemaModifier.Modifier<ViewSchemaMapper<?>> {

        private final Where annotation;

        @Override
        public ViewSchemaMapper<?> modify(final ViewSchemaMapper<?> mapper) {

            final Expression where = Expression.parse(annotation.value());
            return mapper.withWhere(where);
        }

        public static Where from(final Expression where) {

            return new Where() {

                @Override
                public Class<? extends Annotation> annotationType() {

                    return Where.class;
                }

                @Override
                public String value() {

                    return where.toString();
                }
            };
        }
    }
}

package io.basestar.mapper.annotation;

import com.google.common.collect.ImmutableMap;
import io.basestar.mapper.internal.MemberMapper;
import io.basestar.mapper.internal.annotation.MemberModifier;
import io.basestar.type.AnnotationContext;
import lombok.RequiredArgsConstructor;

import java.lang.annotation.*;

@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD, ElementType.METHOD})
@MemberModifier(Expression.Modifier.class)
public @interface Expression {

    String value();

    @RequiredArgsConstructor
    class Modifier implements MemberModifier.Modifier<MemberMapper<?>> {

        private final Expression annotation;

        @Override
        public MemberMapper<?> modify(final MemberMapper<?> mapper) {

            final io.basestar.expression.Expression where = io.basestar.expression.Expression.parse(annotation.value());
            return mapper.withExpression(where);
        }

        public static Expression annotation(final io.basestar.expression.Expression expression) {

            return new AnnotationContext<>(Expression.class, ImmutableMap.<String, Object>builder()
                    .put("value", expression.toString())
                    .build()).annotation();
        }
    }
}

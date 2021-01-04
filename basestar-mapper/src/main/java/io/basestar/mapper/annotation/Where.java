package io.basestar.mapper.annotation;

import com.google.common.collect.ImmutableMap;
import io.basestar.expression.Expression;
import io.basestar.mapper.MappingContext;
import io.basestar.mapper.internal.ViewSchemaMapper;
import io.basestar.mapper.internal.annotation.SchemaModifier;
import io.basestar.type.AnnotationContext;
import lombok.RequiredArgsConstructor;

import java.lang.annotation.*;

@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@SchemaModifier(Where.Modifier.class)
public @interface Where {

    String value();

    @RequiredArgsConstructor
    class Modifier implements SchemaModifier.Modifier<ViewSchemaMapper.Builder<?>> {

        private final Where annotation;

        @Override
        public void modify(final MappingContext context, final ViewSchemaMapper.Builder<?> mapper) {

            final Expression where = Expression.parse(annotation.value());
            mapper.setWhere(where);
        }

        public static Where annotation(final io.basestar.expression.Expression expression) {

            return new AnnotationContext<>(Where.class, ImmutableMap.<String, Object>builder()
                    .put("value", expression.toString())
                    .build()).annotation();
        }
    }
}

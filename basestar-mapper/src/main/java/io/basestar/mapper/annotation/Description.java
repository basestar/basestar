package io.basestar.mapper.annotation;

import com.google.common.collect.ImmutableMap;
import io.basestar.mapper.MappingContext;
import io.basestar.mapper.SchemaMapper;
import io.basestar.mapper.internal.MemberMapper;
import io.basestar.mapper.internal.annotation.MemberModifier;
import io.basestar.mapper.internal.annotation.SchemaModifier;
import io.basestar.type.AnnotationContext;
import lombok.RequiredArgsConstructor;

import java.lang.annotation.*;

@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.FIELD, ElementType.METHOD})
@SchemaModifier(Description.Modifier.class)
@MemberModifier(Description.Modifier.class)
public @interface Description {

    String value();

    @RequiredArgsConstructor
    class Modifier implements SchemaModifier.Modifier<SchemaMapper<?, ?>>, MemberModifier.Modifier<MemberMapper<?>> {

        private final Description annotation;

        @Override
        public SchemaMapper<?, ?> modify(final MappingContext context, final SchemaMapper<?, ?> mapper) {

            return mapper.withDescription(annotation.value());
        }

        @Override
        public MemberMapper<?> modify(final MappingContext context, final MemberMapper<?> mapper) {

            return mapper.withDescription(annotation.value());
        }

        public static Description annotation(final String description) {

            return new AnnotationContext<>(Description.class, ImmutableMap.<String, Object>builder()
                    .put("value", description)
                    .build()).annotation();
        }
    }
}

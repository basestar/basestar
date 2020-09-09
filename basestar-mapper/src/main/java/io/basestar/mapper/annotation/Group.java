package io.basestar.mapper.annotation;

import com.google.common.collect.ImmutableMap;
import io.basestar.mapper.MappingContext;
import io.basestar.mapper.internal.AnnotationUtils;
import io.basestar.mapper.internal.ViewSchemaMapper;
import io.basestar.mapper.internal.annotation.SchemaModifier;
import io.basestar.type.AnnotationContext;
import lombok.RequiredArgsConstructor;

import java.lang.annotation.*;
import java.util.Arrays;
import java.util.List;

@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@SchemaModifier(Group.Modifier.class)
public @interface Group {

    String[] value();

    @RequiredArgsConstructor
    class Modifier implements SchemaModifier.Modifier<ViewSchemaMapper<?>> {

        private final Group annotation;

        @Override
        public ViewSchemaMapper<?> modify(final MappingContext context, final ViewSchemaMapper<?> mapper) {

            final List<String> group = Arrays.asList(annotation.value());
            return mapper.withGroup(group);
        }

        public static Group annotation(final List<String> group) {

            return new AnnotationContext<>(Group.class, ImmutableMap.<String, Object>builder()
                    .put("value", AnnotationUtils.stringArray(group))
                    .build()).annotation();
        }
    }
}

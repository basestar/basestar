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
    class Modifier implements SchemaModifier.Modifier<ViewSchemaMapper.Builder<?>> {

        private final Group annotation;

        @Override
        public void modify(final MappingContext context, final ViewSchemaMapper.Builder<?> mapper) {

            final List<String> group = Arrays.asList(annotation.value());
            mapper.setGroup(group);
        }

        public static Group annotation(final List<String> group) {

            return new AnnotationContext<>(Group.class, ImmutableMap.<String, Object>builder()
                    .put("value", AnnotationUtils.stringArray(group))
                    .build()).annotation();
        }
    }
}

package io.basestar.mapper.annotation;

import io.basestar.mapper.internal.ViewSchemaMapper;
import io.basestar.mapper.internal.annotation.SchemaModifier;
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
        public ViewSchemaMapper<?> modify(final ViewSchemaMapper<?> mapper) {

            final List<String> group = Arrays.asList(annotation.value());
            return mapper.withGroup(group);
        }

        public static Group from(final List<String> group) {

            return new Group() {

                @Override
                public Class<? extends Annotation> annotationType() {

                    return Group.class;
                }

                @Override
                public String[] value() {

                    return group.toArray(new String[0]);
                }
            };
        }
    }
}

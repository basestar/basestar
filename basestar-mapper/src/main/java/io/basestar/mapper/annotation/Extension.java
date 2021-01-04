package io.basestar.mapper.annotation;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.basestar.jackson.BasestarModule;
import io.basestar.mapper.MappingContext;
import io.basestar.mapper.SchemaMapper;
import io.basestar.mapper.internal.annotation.SchemaModifier;
import io.basestar.type.AnnotationContext;
import lombok.RequiredArgsConstructor;

import java.io.IOException;
import java.lang.annotation.*;

@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
@Repeatable(Extension.Multi.class)
@SchemaModifier(Extension.Modifier.class)
public @interface Extension {

    String name();

    String jsonValue();

    @Documented
    @Retention(RetentionPolicy.RUNTIME)
    @Target({ElementType.TYPE})
    @SchemaModifier(Extension.Multi.Modifier.class)
    @interface Multi {

        Extension[] value();

        @RequiredArgsConstructor
        class Modifier implements SchemaModifier.Modifier<SchemaMapper.Builder<?, ?>> {

            private final Multi annotation;

            @Override
            public void modify(final MappingContext context, final SchemaMapper.Builder<?, ?> mapper) {

                Extension.Modifier.modify(mapper, annotation.value());
            }
        }
    }

    @RequiredArgsConstructor
    class Modifier implements SchemaModifier.Modifier<SchemaMapper.Builder<?, ?>> {

        private static final ObjectMapper objectMapper = new ObjectMapper().registerModule(BasestarModule.INSTANCE);

        private final Extension annotation;

        @Override
        public void modify(final MappingContext context, final SchemaMapper.Builder<?, ?> mapper) {

            modify(mapper, annotation);
        }

        private static void modify(final SchemaMapper.Builder<?, ?> mapper, final Extension ... annotations) {

        }

        public static Extension annotation(final String name, final Object value) {

            try {
                return new AnnotationContext<>(Extension.class, ImmutableMap.<String, Object>builder()
                        .put("name", name)
                        .put("jsonValue", objectMapper.writeValueAsString(value))
                        .build()).annotation();
            } catch (final IOException e) {
                throw new IllegalStateException(e);
            }
        }
    }
}

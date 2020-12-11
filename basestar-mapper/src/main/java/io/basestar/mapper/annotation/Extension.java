package io.basestar.mapper.annotation;

import io.basestar.mapper.MappingContext;
import io.basestar.mapper.internal.ObjectSchemaMapper;
import io.basestar.mapper.internal.annotation.SchemaModifier;
import lombok.RequiredArgsConstructor;

import java.lang.annotation.*;

@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
@Repeatable(Extension.Multi.class)
@SchemaModifier(Index.Modifier.class)
public @interface Extension {

    String name();

    String jsonValue();

    @Documented
    @Retention(RetentionPolicy.RUNTIME)
    @Target({ElementType.TYPE})
    @SchemaModifier(Index.Multi.Modifier.class)
    @interface Multi {

        Extension[] value();

        @RequiredArgsConstructor
        class Modifier implements SchemaModifier.Modifier<ObjectSchemaMapper<?>> {

            private final Extension.Multi annotation;

            @Override
            public ObjectSchemaMapper<?> modify(final MappingContext context, final ObjectSchemaMapper<?> mapper) {

                return null;
//                return Index.Modifier.modify(mapper, annotation.value());
            }
        }
    }
}

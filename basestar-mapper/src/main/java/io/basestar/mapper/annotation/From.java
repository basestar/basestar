package io.basestar.mapper.annotation;

import com.google.common.collect.ImmutableMap;
import io.basestar.mapper.MappingContext;
import io.basestar.mapper.internal.AnnotationUtils;
import io.basestar.mapper.internal.TypeMapper;
import io.basestar.mapper.internal.ViewSchemaMapper;
import io.basestar.mapper.internal.annotation.SchemaModifier;
import io.basestar.type.AnnotationContext;
import io.basestar.type.TypeContext;
import io.basestar.util.Name;
import lombok.RequiredArgsConstructor;

import java.lang.annotation.*;
import java.util.Set;

@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@SchemaModifier(From.Modifier.class)
public @interface From {

    Class<?> value() default Object.class;

    String schema() default "";

    String[] expand() default {};

    @RequiredArgsConstructor
    class Modifier implements SchemaModifier.Modifier<ViewSchemaMapper<?>> {

        private final From annotation;

        @Override
        public ViewSchemaMapper<?> modify(final MappingContext context, final ViewSchemaMapper<?> mapper) {

            final Name fromSchema;
            if(!annotation.schema().isEmpty()) {
                fromSchema = Name.parse(annotation.schema());
            } else {
                final TypeMapper.OfCustom tmp = new TypeMapper.OfCustom(context, TypeContext.from(annotation.value()));
                fromSchema = tmp.getQualifiedName();
            }
            final Set<Name> fromExpand = Name.parseSet(annotation.expand());
            return mapper.withFrom(fromSchema, fromExpand);
        }

        public static From annotation(final io.basestar.schema.ViewSchema.From from) {

            return new AnnotationContext<>(From.class, ImmutableMap.<String, Object>builder()
                    .put("value", Object.class)
                    .put("schema", from.getSchema().getQualifiedName().toString())
                    .put("expand", AnnotationUtils.stringArray(from.getExpand()))
                    .build()).annotation();
        }
    }
}

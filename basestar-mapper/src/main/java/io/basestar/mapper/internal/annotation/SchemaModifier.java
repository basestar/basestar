package io.basestar.mapper.internal.annotation;

import io.basestar.mapper.MappingContext;
import io.basestar.mapper.SchemaMapper;

import java.lang.annotation.*;

@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.ANNOTATION_TYPE)
public @interface SchemaModifier {

    Class<? extends Modifier<?>> value();

    interface Modifier<M extends SchemaMapper<?, ?>> {

        M modify(MappingContext context, M mapper);
    }
}
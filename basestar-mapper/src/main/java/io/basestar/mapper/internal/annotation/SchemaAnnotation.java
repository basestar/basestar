package io.basestar.mapper.internal.annotation;

import io.basestar.mapper.internal.SchemaBinder;

import java.lang.annotation.*;

@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.ANNOTATION_TYPE)
public @interface SchemaAnnotation {

    Class<? extends SchemaBinder> value();
}

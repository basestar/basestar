package io.basestar.mapper.internal.annotation;

import io.basestar.mapper.internal.PropertyBinder;

import java.lang.annotation.*;

@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.ANNOTATION_TYPE)
public @interface PropertyAnnotation {

    Class<? extends PropertyBinder> value();
}

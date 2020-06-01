package io.basestar.mapper.internal.annotation;

import java.lang.annotation.*;

@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.ANNOTATION_TYPE)
public @interface Bind {

    Class<? extends Binder<?, ?>> value();

    interface Binder<PARENT, CONTEXT> {

//        String name(CONTEXT context);

        void bindSchema(PARENT parent, CONTEXT context);
    }
}

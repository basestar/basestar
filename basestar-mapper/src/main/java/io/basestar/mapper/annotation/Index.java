package io.basestar.mapper.annotation;

import java.lang.annotation.*;

@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
@Repeatable(Index.Multi.class)
public @interface Index {

    String name();

    String[] partition() default {};

    String[] sort() default {};

    boolean unique() default false;

    Over[] over() default {};

    String[] projection() default {};

    @Documented
    @Retention(RetentionPolicy.RUNTIME)
    @Target({ElementType.TYPE})
    @interface Multi {

        Index[] value();
    }

    @Documented
    @Retention(RetentionPolicy.RUNTIME)
    @interface Over {

        String as();

        String path();
    }
}
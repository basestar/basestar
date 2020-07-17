package io.basestar.mapper.internal.annotation;

import io.basestar.mapper.internal.MemberMapper;

import java.lang.annotation.*;

@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.ANNOTATION_TYPE)
public @interface MemberModifier {

    Class<? extends Modifier<?>> value();

    interface Modifier<M extends MemberMapper<?>> {

        M modify(M mapper);
    }
}
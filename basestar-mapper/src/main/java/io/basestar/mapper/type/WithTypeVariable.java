package io.basestar.mapper.type;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;

import java.lang.reflect.AnnotatedType;
import java.lang.reflect.TypeVariable;

@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PROTECTED)
public class WithTypeVariable<T> implements HasName, HasType<T> {

    private final TypeVariable<? extends Class<?>> variable;

    private final AnnotatedType annotatedType;

    @Override
    public String name() {

        return variable.getName();
    }
}

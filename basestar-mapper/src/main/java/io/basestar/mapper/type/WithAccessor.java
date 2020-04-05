package io.basestar.mapper.type;

import java.lang.reflect.InvocationTargetException;

public interface WithAccessor<T, V> extends HasName, HasAnnotations, HasModifiers, HasType<V> {

    boolean canGet();

    boolean canSet();

    V get(T parent) throws IllegalAccessException, InvocationTargetException;

    void set(T parent, V value) throws IllegalAccessException, InvocationTargetException;
}

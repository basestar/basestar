package io.basestar.type;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;

public interface SerializableAccessor extends Serializable {

    boolean canGet();

    boolean canSet();

    <T, V> V get(T target) throws IllegalAccessException, InvocationTargetException;

    <T, V> void set(T target, V value) throws IllegalAccessException, InvocationTargetException;
}

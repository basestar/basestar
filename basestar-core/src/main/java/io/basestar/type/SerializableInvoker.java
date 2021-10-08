package io.basestar.type;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;

public interface SerializableInvoker extends Serializable {

    <T, V> V invoke(T target, Object... args) throws InvocationTargetException, IllegalAccessException;
}

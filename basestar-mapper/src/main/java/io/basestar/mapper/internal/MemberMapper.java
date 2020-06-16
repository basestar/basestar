package io.basestar.mapper.internal;

import io.basestar.schema.InstanceSchema;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.Set;

public interface MemberMapper<B extends InstanceSchema.Builder> {

    TypeMapper getType();

    void addToSchema(B builder);

    void unmarshall(Object source, Map<String, Object> target) throws InvocationTargetException, IllegalAccessException;

    void marshall(Map<String, Object> source, Object target) throws InvocationTargetException, IllegalAccessException;

    default Set<Class<?>> dependencies() {

        return getType().dependencies();
    }
}

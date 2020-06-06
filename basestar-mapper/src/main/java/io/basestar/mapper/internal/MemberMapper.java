package io.basestar.mapper.internal;

import io.basestar.schema.InstanceSchema;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;

public interface MemberMapper<B extends InstanceSchema.Builder> {

    void addToSchema(B builder);

    void unmarshall(Object source, Map<String, Object> target) throws InvocationTargetException, IllegalAccessException;

    void marshall(Map<String, Object> source, Object target) throws InvocationTargetException, IllegalAccessException;
}

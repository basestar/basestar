package io.basestar.mapper;

import io.basestar.schema.Schema;

import java.util.Set;

public interface SchemaMapper<T, O> {

    String name();

    Schema.Builder<? extends O> schema();

    T marshall(Object value);

    O unmarshall(T value);

    Set<Class<?>> dependencies();
}

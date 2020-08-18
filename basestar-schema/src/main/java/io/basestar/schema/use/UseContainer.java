package io.basestar.schema.use;

import java.util.function.Function;

public interface UseContainer<V, T> extends Use<T> {

    Use<V> getType();

    T transform(T value, Function<V, V> fn);

    <V2> UseContainer<V2, ?> transform(Function<Use<V>, Use<V2>> fn);
}

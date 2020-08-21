package io.basestar.schema.use;

import java.util.function.BiFunction;
import java.util.function.Function;

public interface UseContainer<V, T> extends Use<T> {

    Use<V> getType();

    default T transformValues(final T value, final Function<V, V> fn) {

        return transformValues(value, (t, v) -> fn.apply(v));
    }

    T transformValues(T value, BiFunction<Use<V>, V, V> fn);

    <V2> UseContainer<V2, ?> transform(Function<Use<V>, Use<V2>> fn);
}

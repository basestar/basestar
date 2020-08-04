package io.basestar.schema.use;

import java.util.function.Function;

public interface UseContainer<V, T> extends Use<T> {

    Use<?> transform(Function<Use<V>, Use<?>> fn);
}

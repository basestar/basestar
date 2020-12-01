package io.basestar.schema.use;

import java.util.Objects;

public interface UseNumeric<T extends Number> extends UseScalar<T> {

    @Override
    default boolean areEqual(final T a, final T b) {

        return Objects.equals(a, b);
    }
}

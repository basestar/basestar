package io.basestar.schema.use;

import java.util.Objects;

public interface UseStringLike<T> extends UseScalar<T> {

    @Override
    default boolean areEqual(final T a, final T b) {

        return Objects.equals(a, b);
    }

    @Override
    default boolean isStringLike() {

        return true;
    }
}

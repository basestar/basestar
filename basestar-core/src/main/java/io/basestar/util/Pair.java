package io.basestar.util;

import lombok.Data;

import java.io.Serializable;

public interface Pair<T> extends Serializable {

    T getFirst();

    T getSecond();

    static <T> Simple<T> of(final T first, final T second) {

        return new Simple<>(first, second);
    }

    @Data
    class Simple<T> implements Pair<T> {

        private final T first;

        private final T second;
    }
}

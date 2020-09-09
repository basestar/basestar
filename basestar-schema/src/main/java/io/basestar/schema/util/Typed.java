package io.basestar.schema.util;

import io.basestar.schema.use.Use;
import io.basestar.util.Pair;
import lombok.Data;

@Data
public class Typed<T> implements Pair<Use<T>, T> {

    private final Use<T> type;

    private final T value;

    @Override
    public Use<T> getFirst() {

        return type;
    }

    @Override
    public T getSecond() {

        return value;
    }

    @Override
    public Pair<T, Use<T>> swap() {

        return Pair.of(value, type);
    }
}

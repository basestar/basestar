package io.basestar.expression.methods;

import io.basestar.expression.type.Values;

import java.util.Map;

public class MapMethods {

    public int size(final Map<?, ?> target) {

        return target.size();
    }

    public boolean isEmpty(final Map<?, ?> target) {

        return target.isEmpty();
    }

    public boolean containsKey(final Map<?, ?> target, final Object o) {

        return target.keySet().stream().anyMatch(v -> Values.equals(v, o));
    }

    public boolean containsValue(final Map<?, ?> target, final Object o) {

        return target.values().stream().anyMatch(v -> Values.equals(v, o));
    }

}

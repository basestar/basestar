package io.basestar.spark.util;

import org.apache.spark.util.AccumulatorV2;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class SetAccumulator<T> extends AccumulatorV2<Set<T>, Set<T>> {

    private final Set<T> value;

    public SetAccumulator() {

        this.value = new HashSet<>();
    }

    public SetAccumulator(final Set<T> value) {

        this.value = new HashSet<>(value);
    }

    @Override
    public boolean isZero() {

        return value.isEmpty();
    }

    @Override
    public SetAccumulator<T> copy() {

        return new SetAccumulator<>(value);
    }

    @Override
    public void reset() {

        value.clear();
    }

    @Override
    public void add(final Set<T> v) {

        value.addAll(v);
    }

    public void inc(final T v) {

        value.add(v);
    }

    @Override
    public void merge(final AccumulatorV2<Set<T>, Set<T>> other) {

        add(other.value());
    }

    @Override
    public Set<T> value() {

        return Collections.unmodifiableSet(value);
    }
}

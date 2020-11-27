package io.basestar.spark.sink;

import com.google.common.collect.ImmutableList;
import io.basestar.util.Nullsafe;

import java.util.List;

public class MultiSink<T> implements Sink<T> {

    private final List<Sink<T>> sinks;

    @SafeVarargs
    public MultiSink(final Sink<T> ... sinks) {

        this.sinks = ImmutableList.copyOf(sinks);
    }

    public MultiSink(final List<Sink<T>> sinks) {

        this.sinks = Nullsafe.immutableCopy(sinks);
    }

    @Override
    public void accept(final T input) {

        sinks.forEach(sink -> sink.accept(input));
    }
}

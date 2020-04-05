package io.basestar.spark;

import java.io.Serializable;

public interface Transform<I, O> extends Serializable {

    O apply(I input);

    default <O2> Transform<I, O2> then(final Transform<O, O2> next) {

        return input -> next.apply(apply(input));
    }

    default Sink<I> then(final Sink<O> next) {

        return input -> next.accept(apply(input));
    }
}

package io.basestar.spark;

import java.io.Serializable;

public interface Sink<I> extends Serializable {

    void accept(I input);
}

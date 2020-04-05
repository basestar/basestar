package io.basestar.spark;

import java.io.Serializable;

public interface Source<I> extends Serializable {

    void sink(Sink<I> sink);
}

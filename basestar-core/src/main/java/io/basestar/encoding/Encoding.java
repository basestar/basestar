package io.basestar.encoding;

public interface Encoding<I, O> {

    O encode(I v);

    I decode(O v);
}

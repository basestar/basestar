package io.basestar.util;

import java.util.Iterator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

// Because we must be compatible with Guava 14.0.1

public class Streams {

    private Streams() {

    }

    public static <T> Stream<T> stream(final Iterable<T> iterable) {

        return StreamSupport.stream(iterable.spliterator(), false);
    }

    public static <T> Stream<T> stream(final Iterator<T> iterator) {

        final Iterable<T> iterable = () -> iterator;
        return stream(iterable);
    }
}

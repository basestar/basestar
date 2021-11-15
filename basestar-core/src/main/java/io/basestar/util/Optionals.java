package io.basestar.util;

import java.util.Optional;
import java.util.function.BiFunction;
import java.util.stream.Stream;

public class Optionals {

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    public static <A, B, R> Optional<R> map(final Optional<A> a, final Optional<B> b, final BiFunction<A, B, R> then) {

        if (a.isPresent() && b.isPresent()) {
            return Optional.of(then.apply(a.get(), b.get()));
        } else {
            return Optional.empty();
        }
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    public static <T> Stream<T> stream(final Optional<T> opt) {
        return opt.map(Stream::of).orElseGet(Stream::empty);
    }
}

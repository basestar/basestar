package io.basestar.util;

import java.util.Optional;

public class Throwables {

    public static <X> Optional<X> find(final Throwable e, final Class<X> of) {

        if(of.isAssignableFrom(e.getClass())) {
            return Optional.of(of.cast(e));
        } else if(e.getCause() != null) {
            return find(e.getCause(), of);
        } else {
            return Optional.empty();
        }
    }

    public static Optional<RuntimeException> runtimeCause(final Throwable e) {

        return find(e, RuntimeException.class);
    }
}

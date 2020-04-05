package io.basestar.mapper.type;

import java.util.function.Predicate;
import java.util.regex.Pattern;

public interface HasName {

    String name();

    default String simpleName() {

        return name();
    }

    static Predicate<HasName> match(final String name) {

        return v -> v.name().equals(name);
    }

    static Predicate<HasName> match(final Pattern pattern) {

        return v -> pattern.matcher(v.name()).matches();
    }
}

package io.basestar.mapper.type;

import java.util.function.Predicate;

public interface HasModifiers {

    int modifiers();

    static Predicate<HasModifiers> match(final int modifiers) {

        return v -> (v.modifiers() & modifiers) == modifiers;
    }

    static Predicate<HasModifiers> matchNot(final int modifiers) {

        return v -> (v.modifiers() & modifiers) == 0;
    }
}

package io.basestar.expression.methods;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SetMethods extends CollectionMethods<Set<?>> {

    protected Set<?> collect(final Stream<?> stream) {

        return stream.collect(Collectors.toSet());
    }
}

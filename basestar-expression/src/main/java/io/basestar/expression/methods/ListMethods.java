package io.basestar.expression.methods;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ListMethods extends CollectionMethods<List<?>> {

    protected List<?> collect(final Stream<?> stream) {

        return stream.collect(Collectors.toList());
    }
}

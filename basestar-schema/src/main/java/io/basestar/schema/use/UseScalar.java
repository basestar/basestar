package io.basestar.schema.use;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import io.basestar.schema.Expander;
import io.basestar.schema.Instance;
import io.basestar.schema.Schema;
import io.basestar.util.Path;

import java.util.Collections;
import java.util.Set;

public interface UseScalar<T> extends Use<T> {

    @Override
    default Use<T> resolve(final Schema.Resolver resolver) {

        return this;
    }

    @Override
    default T expand(final T value, final Expander expander, final Set<Path> expand) {

        return value;
    }

    @Override
    @Deprecated
    default Set<Path> requireExpand(final Set<Path> paths) {

        return Collections.emptySet();
    }

    @Override
    @Deprecated
    default Multimap<Path, Instance> refs(final T value) {

        return HashMultimap.create();
    }

    @Override
    default Use<?> typeOf(final Path path) {

        if(path.isEmpty()) {
            return this;
        } else {
            throw new IllegalStateException();
        }
    }
}

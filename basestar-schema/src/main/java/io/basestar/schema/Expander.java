package io.basestar.schema;

import io.basestar.util.PagedList;
import io.basestar.util.Path;

import java.util.Set;

public interface Expander {

    static Expander noop() {

        return new Expander() {
            @Override
            public Instance ref(final ObjectSchema schema, final Instance ref, final Set<Path> expand) {

                return ref;
            }

            @Override
            public PagedList<Instance> link(final Link link, final PagedList<Instance> value, final Set<Path> expand) {

                return value;
            }
        };
    }

    Instance ref(ObjectSchema schema, Instance ref, Set<Path> expand);

    PagedList<Instance> link(Link link, PagedList<Instance> value, Set<Path> expand);
}

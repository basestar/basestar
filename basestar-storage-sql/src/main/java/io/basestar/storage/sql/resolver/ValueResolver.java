package io.basestar.storage.sql.resolver;

import io.basestar.util.Name;

public interface ValueResolver {

    Object value();

    Object value(Name name);

    static ValueResolver of(final Object value) {

        return new ValueResolver() {
            @Override
            public Object value() {

                return value;
            }

            @Override
            public Object value(final Name name) {

                throw new UnsupportedOperationException("Subfield " + name + " not resolvable");
            }
        };
    }

    default ValueResolver resolver(final Name root) {

        final ValueResolver delegate = this;
        return new ValueResolver() {
            @Override
            public Object value() {

                return value(Name.empty());
            }

            @Override
            public Object value(final Name name) {

                return delegate.value(root.with(name));
            }
        };
    }
}

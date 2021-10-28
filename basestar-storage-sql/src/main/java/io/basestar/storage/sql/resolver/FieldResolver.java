package io.basestar.storage.sql.resolver;

import io.basestar.util.Name;
import org.jooq.Field;

import java.util.Optional;

public interface FieldResolver {

    Optional<Field<?>> field();

    Optional<Field<?>> field(Name name);

    default FieldResolver resolver(final Name root) {

        final FieldResolver delegate = this;
        return new FieldResolver() {
            @Override
            public Optional<Field<?>> field() {

                return field(Name.empty());
            }

            @Override
            public Optional<Field<?>> field(final Name name) {

                return delegate.field(root.with(name));
            }
        };
    }
}

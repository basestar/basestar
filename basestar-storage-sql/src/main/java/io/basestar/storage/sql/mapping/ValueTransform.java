package io.basestar.storage.sql.mapping;

import io.basestar.schema.use.Use;
import io.basestar.util.Name;
import lombok.Data;

import java.util.Set;
import java.util.function.Function;

public interface ValueTransform<T, S> {

    S toSQLValue(T value);

    T fromSQLValue(S value);

    default <R> ValueTransform<R, S> then(final ValueTransform<R, T> transform) {

        return new ValueTransform<R, S>() {
            @Override
            public S toSQLValue(final R value) {

                final T first = transform.toSQLValue(value);
                return ValueTransform.this.toSQLValue(first);
            }

            @Override
            public R fromSQLValue(final S value) {

                final T first = ValueTransform.this.fromSQLValue(value);
                return transform.fromSQLValue(first);
            }
        };
    }

    @Data
    class Coercing<T, S> implements ValueTransform<T, S> {

        private final Use<T> from;

        private final Set<Name> expand;

        private final Function<T, S> to;

        @Override
        public S toSQLValue(final T value) {

            return to.apply(value);
        }

        @Override
        public T fromSQLValue(final S value) {

            return from.create(value, expand);
        }
    }
}

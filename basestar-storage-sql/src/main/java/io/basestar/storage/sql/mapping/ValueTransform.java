package io.basestar.storage.sql.mapping;

import io.basestar.util.Name;
import lombok.Data;
import org.jooq.SelectField;
import org.jooq.impl.DSL;

import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

public interface ValueTransform<T, S> {

    SelectField<S> toSQLValue(T value);

    T fromSQLValue(S value, Set<Name> expand);

    static <T> ValueTransform<T, T> noop() {

        return new ValueTransform<T, T>() {
            @Override
            public SelectField<T> toSQLValue(final T value) {

                return DSL.inline(value);
            }

            @Override
            public T fromSQLValue(final T value, final Set<Name> expand) {

                return value;
            }
        };
    }

    @Data
    class Coercing<T, S> implements ValueTransform<T, S> {

        private final BiFunction<S, Set<Name>, T> from;

        private final Function<T, S> to;

        @Override
        public SelectField<S> toSQLValue(final T value) {

            return DSL.inline(to.apply(value));
        }

        @Override
        public T fromSQLValue(final S value, final Set<Name> expand) {

            return from.apply(value, expand);
        }
    }
}

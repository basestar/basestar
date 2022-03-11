package io.basestar.storage.sql.mapping;

import io.basestar.util.Name;
import lombok.Data;

import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

public interface ValueTransform<T, S> {

    S toSQLValue(T value);

    T fromSQLValue(S value, Set<Name> expand);

    static <T> ValueTransform<T, T> noop() {

        return new ValueTransform<T, T>() {
            @Override
            public T toSQLValue(final T value) {

                return value;
            }

            @Override
            public T fromSQLValue(final T value, final Set<Name> expand) {

                return value;
            }
        };
    }

//    default <R> ValueTransform<R, S> then(final ValueTransform<R, T> transform) {
//
//        return new ValueTransform<R, S>() {
//            @Override
//            public S toSQLValue(final R value) {
//
//                final T first = transform.toSQLValue(value);
//                return ValueTransform.this.toSQLValue(first);
//            }
//
//            @Override
//            public R fromSQLValue(final S value, final Set<Name> expand) {
//
//                final T first = ValueTransform.this.fromSQLValue(value, expand);
//                return transform.fromSQLValue(first, expand);
//            }
//        };
//    }

    @Data
    class Coercing<T, S> implements ValueTransform<T, S> {

        private final BiFunction<S, Set<Name>, T> from;

        private final Function<T, S> to;

        @Override
        public S toSQLValue(final T value) {

            return to.apply(value);
        }

        @Override
        public T fromSQLValue(final S value, final Set<Name> expand) {

            return from.apply(value, expand);
        }
    }
}

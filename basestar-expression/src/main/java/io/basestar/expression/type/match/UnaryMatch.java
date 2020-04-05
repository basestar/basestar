package io.basestar.expression.type.match;

import io.basestar.expression.type.Values;
import io.basestar.expression.type.exception.BadOperandsException;

import java.util.Collection;
import java.util.Map;

public interface UnaryMatch<T> {

    default T defaultApply(Object value) {

        throw new BadOperandsException(this + " cannot be applied to " + Values.className(value));
    }

    default T apply(final Void value) {

        return defaultApply(value);
    }

    default T apply(final Boolean value) {

        return defaultApply(value);
    }

    default T apply(final Number value) {

        return defaultApply(value);
    }

    default T apply(final String value) {

        return defaultApply(value);
    }

    default T apply(final Collection<?> value) {

        return defaultApply(value);
    }

    default T apply(final Map<?, ?> value) {

        return defaultApply(value);
    }

    default T apply(final Object value) {

        if(value == null) {
            return apply((Void)null);
        } else if(value instanceof Boolean) {
            return apply((Boolean)value);
        } else if(value instanceof Number) {
            return apply((Number)value);
        } else if(value instanceof String) {
            return apply((String)value);
        } else if(value instanceof Collection<?>) {
            return apply((Collection<?>)value);
        } else if(value instanceof Map<?, ?>) {
            return apply((Map<?, ?>)value);
        } else {
            return defaultApply(value);
        }
    }
}
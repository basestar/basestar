package io.basestar.expression.type.match;

import io.basestar.expression.type.Values;
import io.basestar.expression.type.exception.BadOperandsException;

public interface UnaryNumberMatch<T> {

    default T defaultApply(Object value) {

        throw new BadOperandsException(this + " cannot be applied to " + value.getClass());
    }

    default T apply(final Long value) {

        return defaultApply(value);
    }

    default T apply(final Double value) {

        return defaultApply(value);
    }

    default T apply(final Number value) {

        if(Values.isInteger(value)) {
            return apply(value.longValue());
        } else {
            return apply(value.doubleValue());
        }
    }
}
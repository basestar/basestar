package io.basestar.expression.type.match;

import io.basestar.expression.type.Values;
import io.basestar.expression.type.exception.BadOperandsException;

public interface BinaryNumberMatch<T> {

    default T defaultApply(final Number lhs, final Number rhs) {

        throw new BadOperandsException(this + " cannot be applied to " + lhs.getClass() + " and " + rhs.getClass());
    }

    default <U extends Number> T defaultApplySame(final U lhs, final U rhs) {

        return defaultApply(lhs, rhs);
    }

    default T apply(final Long lhs, final Long rhs) {

        return defaultApplySame(lhs, rhs);
    }

    default T apply(final Long lhs, final Double rhs) {

        return defaultApply(lhs, rhs);
    }

    default T apply(final Double lhs, final Long rhs) {

        return defaultApply(lhs, rhs);
    }

    default T apply(final Double lhs, final Double rhs) {

        return defaultApplySame(lhs, rhs);
    }

    default T apply(final Long lhs, final Number rhs) {

        if (Values.isInteger(rhs)) {
            return apply(lhs, rhs.longValue());
        } else {
            return apply(lhs, rhs.doubleValue());
        }
    }

    default T apply(final Double lhs, final Number rhs) {

        if (Values.isInteger(rhs)) {
            return apply(lhs, rhs.longValue());
        } else {
            return apply(lhs, rhs.doubleValue());
        }
    }

    default T apply(final Number lhs, final Number rhs) {

        if (Values.isInteger(lhs)) {
            return apply(lhs.longValue(), rhs);
        } else {
            return apply(lhs.doubleValue(), rhs);
        }
    }

    interface Promoting<T> extends BinaryNumberMatch<T> {

        default T apply(final Long lhs, final Double rhs) {

            return apply(lhs.doubleValue(), rhs);
        }

        default T apply(final Double lhs, final Long rhs) {

            return apply(lhs, rhs.doubleValue());
        }
    }
}

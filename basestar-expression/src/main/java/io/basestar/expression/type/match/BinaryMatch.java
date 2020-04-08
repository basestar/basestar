package io.basestar.expression.type.match;

/*-
 * #%L
 * basestar-expression
 * $Id:$
 * $HeadURL:$
 * %%
 * Copyright (C) 2020 basestar.io
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import io.basestar.expression.type.Values;
import io.basestar.expression.type.exception.BadOperandsException;

import java.util.Collection;
import java.util.Map;

public interface BinaryMatch<T> {

    default T defaultApply(final Object lhs, final Object rhs) {

        throw new BadOperandsException(this + " cannot be applied to " + Values.className(lhs) + " and " + Values.className(rhs));
    }

    default <U> T defaultApplySame(final U lhs, final U rhs) {

        return defaultApply(lhs, rhs);
    }

    default T apply(final Void lhs, final Void rhs) {

        return defaultApplySame(lhs, rhs);
    }

    default T apply(final Void lhs, final Boolean rhs) {

        return defaultApply(lhs, rhs);
    }

    default T apply(final Void lhs, final String rhs) {

        return defaultApply(lhs, rhs);
    }

    default T apply(final Void lhs, final Number rhs) {

        return defaultApply(lhs, rhs);
    }

    default T apply(final Void lhs, final Collection<?> rhs) {

        return defaultApply(lhs, rhs);
    }

    default T apply(final Void lhs, final Map<?, ?> rhs) {

        return defaultApply(lhs, rhs);
    }

    default T apply(final Boolean lhs, final Void rhs) {

        return defaultApply(lhs, rhs);
    }

    default T apply(final Boolean lhs, final Boolean rhs) {

        return defaultApplySame(lhs, rhs);
    }

    default T apply(final Boolean lhs, final String rhs) {

        return defaultApply(lhs, rhs);
    }

    default T apply(final Boolean lhs, final Number rhs) {

        return defaultApply(lhs, rhs);
    }

    default T apply(final Boolean lhs, final Collection<?> rhs) {

        return defaultApply(lhs, rhs);
    }

    default T apply(final Boolean lhs, final Map<?, ?> rhs) {

        return defaultApply(lhs, rhs);
    }

    default T apply(final String lhs, final Void rhs) {

        return defaultApply(lhs, rhs);
    }

    default T apply(final String lhs, final Boolean rhs) {

        return defaultApply(lhs, rhs);
    }

    default T apply(final String lhs, final String rhs) {

        return defaultApplySame(lhs, rhs);
    }

    default T apply(final String lhs, final Number rhs) {

        return defaultApply(lhs, rhs);
    }

    default T apply(final String lhs, final Collection<?> rhs) {

        return defaultApply(lhs, rhs);
    }

    default T apply(final String lhs, final Map<?, ?> rhs) {

        return defaultApply(lhs, rhs);
    }

    default T apply(final Number lhs, final Void rhs) {

        return defaultApply(lhs, rhs);
    }

    default T apply(final Number lhs, final Boolean rhs) {

        return defaultApply(lhs, rhs);
    }

    default T apply(final Number lhs, final String rhs) {

        return defaultApply(lhs, rhs);
    }

    default T apply(final Number lhs, final Number rhs) {

        return defaultApplySame(lhs, rhs);
    }

    default T apply(final Number lhs, final Collection<?> rhs) {

        return defaultApply(lhs, rhs);
    }

    default T apply(final Number lhs, final Map<?, ?> rhs) {

        return defaultApply(lhs, rhs);
    }

    default T apply(final Collection<?> lhs, final Void rhs) {

        return defaultApply(lhs, rhs);
    }

    default T apply(final Collection<?> lhs, final Boolean rhs) {

        return defaultApply(lhs, rhs);
    }

    default T apply(final Collection<?> lhs, final String rhs) {

        return defaultApply(lhs, rhs);
    }

    default T apply(final Collection<?> lhs, final Number rhs) {

        return defaultApply(lhs, rhs);
    }

    default T apply(final Collection<?> lhs, final Collection<?> rhs) {

        return defaultApplySame(lhs, rhs);
    }

    default T apply(final Collection<?> lhs, final Map<?, ?> rhs) {

        return defaultApply(lhs, rhs);
    }

    default T apply(final Map<?, ?> lhs, final Void rhs) {

        return defaultApply(lhs, rhs);
    }

    default T apply(final Map<?, ?> lhs, final Boolean rhs) {

        return defaultApply(lhs, rhs);
    }

    default T apply(final Map<?, ?> lhs, final String rhs) {

        return defaultApply(lhs, rhs);
    }

    default T apply(final Map<?, ?> lhs, final Number rhs) {

        return defaultApply(lhs, rhs);
    }

    default T apply(final Map<?, ?> lhs, final Collection<?> rhs) {

        return defaultApply(lhs, rhs);
    }

    default T apply(final Map<?, ?> lhs, final Map<?, ?> rhs) {

        return defaultApplySame(lhs, rhs);
    }

    default T apply(final Void lhs, final Object rhs) {

        if (rhs == null) {
            return apply(lhs, (Void) null);
        } else if (rhs instanceof Boolean) {
            return apply(lhs, (Boolean) rhs);
        } else if (rhs instanceof Number) {
            return apply(lhs, (Number) rhs);
        } else if (rhs instanceof String) {
            return apply(lhs, (String) rhs);
        } else if (rhs instanceof Collection<?>) {
            return apply(lhs, (Collection<?>) rhs);
        } else if (rhs instanceof Map<?, ?>) {
            return apply(lhs, (Map<?, ?>) rhs);
        } else {
            return defaultApply(lhs, rhs);
        }
    }

    default T apply(final Boolean lhs, final Object rhs) {

        if (rhs == null) {
            return apply(lhs, (Void) null);
        } else if (rhs instanceof Boolean) {
            return apply(lhs, (Boolean) rhs);
        } else if (rhs instanceof Number) {
            return apply(lhs, (Number) rhs);
        } else if (rhs instanceof String) {
            return apply(lhs, (String) rhs);
        } else if (rhs instanceof Collection<?>) {
            return apply(lhs, (Collection<?>) rhs);
        } else if (rhs instanceof Map<?, ?>) {
            return apply(lhs, (Map<?, ?>) rhs);
        } else {
            return defaultApply(lhs, rhs);
        }
    }

    default T apply(final Number lhs, final Object rhs) {

        if (rhs == null) {
            return apply(lhs, (Void) null);
        } else if (rhs instanceof Boolean) {
            return apply(lhs, (Boolean) rhs);
        } else if (rhs instanceof Number) {
            return apply(lhs, (Number) rhs);
        } else if (rhs instanceof String) {
            return apply(lhs, (String) rhs);
        } else if (rhs instanceof Collection<?>) {
            return apply(lhs, (Collection<?>) rhs);
        } else if (rhs instanceof Map<?, ?>) {
            return apply(lhs, (Map<?, ?>) rhs);
        } else {
            return defaultApply(lhs, rhs);
        }
    }

    default T apply(final String lhs, final Object rhs) {

        if (rhs == null) {
            return apply(lhs, (Void) null);
        } else if (rhs instanceof Boolean) {
            return apply(lhs, (Boolean) rhs);
        } else if (rhs instanceof Number) {
            return apply(lhs, (Number) rhs);
        } else if (rhs instanceof String) {
            return apply(lhs, (String) rhs);
        } else if (rhs instanceof Collection<?>) {
            return apply(lhs, (Collection<?>) rhs);
        } else if (rhs instanceof Map<?, ?>) {
            return apply(lhs, (Map<?, ?>) rhs);
        } else {
            return defaultApply(lhs, rhs);
        }
    }

    default T apply(final Collection<?> lhs, final Object rhs) {

        if (rhs == null) {
            return apply(lhs, (Void) null);
        } else if (rhs instanceof Boolean) {
            return apply(lhs, (Boolean) rhs);
        } else if (rhs instanceof Number) {
            return apply(lhs, (Number) rhs);
        } else if (rhs instanceof String) {
            return apply(lhs, (String) rhs);
        } else if (rhs instanceof Collection<?>) {
            return apply(lhs, (Collection<?>) rhs);
        } else if (rhs instanceof Map<?, ?>) {
            return apply(lhs, (Map<?, ?>) rhs);
        } else {
            return defaultApply(lhs, rhs);
        }
    }

    default T apply(final Map<?, ?> lhs, final Object rhs) {

        if (rhs == null) {
            return apply(lhs, (Void) null);
        } else if (rhs instanceof Boolean) {
            return apply(lhs, (Boolean) rhs);
        } else if (rhs instanceof Number) {
            return apply(lhs, (Number) rhs);
        } else if (rhs instanceof String) {
            return apply(lhs, (String) rhs);
        } else if (rhs instanceof Collection<?>) {
            return apply(lhs, (Collection<?>) rhs);
        } else if (rhs instanceof Map<?, ?>) {
            return apply(lhs, (Map<?, ?>) rhs);
        } else {
            return defaultApply(lhs, rhs);
        }
    }

    default T apply(final Object lhs, final Object rhs) {

        if (lhs == null) {
            return apply((Void)null, rhs);
        } else if(lhs instanceof Boolean) {
            return apply((Boolean)lhs, rhs);
        } else if(lhs instanceof Number) {
            return apply((Number)lhs, rhs);
        } else if(lhs instanceof String) {
            return apply((String)lhs, rhs);
        } else if(lhs instanceof Collection<?>) {
            return apply((Collection<?>)lhs, rhs);
        } else if(lhs instanceof Map<?, ?>) {
            return apply((Map<?, ?>)lhs, rhs);
        } else {
            return defaultApply(lhs, rhs);
        }
    }

    interface Promoting<T> extends BinaryMatch<T> {

        default Number toNumber(final Boolean v) {

            return v ? 1 : 0;
        }

        default T apply(final Boolean lhs, final Number rhs) {

            return apply(toNumber(lhs), rhs);
        }

        default T apply(final Number lhs, final Boolean rhs) {

            return apply(lhs, toNumber(rhs));
        }
    }

    interface Coercing<T> extends Promoting<T> {

        default String toString(final Boolean v) {

            return v.toString();
        }

        default String toString(final Number v) {

            return v.toString();
        }

        default T apply(final Boolean lhs, final String rhs) {

            return apply(toString(lhs), rhs);
        }

        default T apply(final String lhs, final Boolean rhs) {

            return apply(lhs, toString(rhs));
        }

        default T apply(final String lhs, final Number rhs) {

            return apply(lhs, toString(rhs));
        }

        default T apply(final Number lhs, final String rhs) {

            return apply(toString(lhs), rhs);
        }
    }
}

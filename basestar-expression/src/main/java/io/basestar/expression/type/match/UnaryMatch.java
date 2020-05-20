package io.basestar.expression.type.match;

/*-
 * #%L
 * basestar-expression
 * %%
 * Copyright (C) 2019 - 2020 Basestar.IO
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

public interface UnaryMatch<T> {

    default T defaultApply(final Object value) {

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

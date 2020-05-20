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

public interface UnaryNumberMatch<T> {

    default T defaultApply(final Object value) {

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

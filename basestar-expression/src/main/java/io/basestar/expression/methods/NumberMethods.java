package io.basestar.expression.methods;

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

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;

@SuppressWarnings("unused")
public class NumberMethods implements Serializable {

    public Number _round(final Number a, final Number places) {

        return BigDecimal.valueOf(a.doubleValue()).setScale(places.intValue(), RoundingMode.HALF_UP).doubleValue();
    }

    public double sqrt(final Number num) {

        return _sqrt(num);
    }

    public long floor(final Number a) {

        return _floor(a);
    }

    public double _sqrt(final Number num) {

        return Math.sqrt(num.doubleValue());
    }

    public long _floor(final Number a) {

        return (long) Math.floor(a.doubleValue());
    }

    public long _ceil(final Number a) {

        return (long) Math.ceil(a.doubleValue());
    }

    public double _pow(final Number a, final Number b) {

        return Math.pow(a.doubleValue(), b.doubleValue());
    }
}

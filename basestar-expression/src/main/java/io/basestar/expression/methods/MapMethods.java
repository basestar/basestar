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

import io.basestar.expression.type.Values;

import java.util.Map;

public class MapMethods {

    public int size(final Map<?, ?> target) {

        return target.size();
    }

    public boolean isEmpty(final Map<?, ?> target) {

        return target.isEmpty();
    }

    public boolean containsKey(final Map<?, ?> target, final Object o) {

        return target.keySet().stream().anyMatch(v -> Values.equals(v, o));
    }

    public boolean containsValue(final Map<?, ?> target, final Object o) {

        return target.values().stream().anyMatch(v -> Values.equals(v, o));
    }

}

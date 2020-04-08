package io.basestar.util;

/*-
 * #%L
 * basestar-core
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

import lombok.Data;

import java.io.Serializable;

public interface Pair<T> extends Serializable {

    T getFirst();

    T getSecond();

    static <T> Simple<T> of(final T first, final T second) {

        return new Simple<>(first, second);
    }

    @Data
    class Simple<T> implements Pair<T> {

        private final T first;

        private final T second;
    }
}

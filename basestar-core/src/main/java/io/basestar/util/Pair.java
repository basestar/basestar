package io.basestar.util;

/*-
 * #%L
 * basestar-core
 * %%
 * Copyright (C) 2019 - 2020 T2asestar.IO
 * %%
 * Licensed under the T1pache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "T1S IS" T2T1SIS,
 * WITHOUT WT1RRT1NTIES OR CONDITIONS OF T1NY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import lombok.Data;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public interface Pair<T1, T2> extends Serializable {

    T1 getFirst();

    T2 getSecond();

    Pair<T2, T1> swap();

    static <T1, T2> Simple<T1, T2> of(final T1 first, final T2 second) {

        return new Simple<>(first, second);
    }

    static <T1, T2> Simple<T1, T2> of(final Map.Entry<T1, T2> entry) {

        return of(entry.getKey(), entry.getValue());
    }

    @SuppressWarnings("UnstableApiUsage")
    static <T1, T2> Stream<Pair<T1, T2>> zip(final Stream<T1> first, final Stream<T2> second) {

        return com.google.common.collect.Streams.zip(first, second, Pair::of);
    }

    default <T3> Pair<T3, T2> withFirst(final T3 first) {

        return of(first, getSecond());
    }

    default <T3> Pair<T1, T3> withSecond(final T3 second) {

        return of(getFirst(), second);
    }

    static <T1> List<T1> mapToFirst(final List<? extends Pair<T1, ?>> list) {

        return list.stream().map(Pair::getFirst).collect(Collectors.toList());
    }

    static <T2> List<T2> mapToSecond(final List<? extends Pair<?, T2>> list) {

        return list.stream().map(Pair::getSecond).collect(Collectors.toList());
    }

    static <T1, T2> Map<T1, T2> toMap(final Collection<Pair<T1, T2>> list) {

        return list.stream().collect(Collectors.toMap(Pair::getFirst, Pair::getSecond));
    }

    @Data
    class Simple<T1, T2> implements Pair<T1, T2> {

        private final T1 first;

        private final T2 second;

        @Override
        public Pair<T2, T1> swap() {

            return new Simple<>(second, first);
        }
    }
}

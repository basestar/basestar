package io.basestar.util;

/*-
 * #%L
 * basestar-core
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

import java.util.*;
import java.util.function.Function;

public class TopologicalSort {

    private TopologicalSort() {

    }

    public static <T> boolean isSorted(final Iterable<? extends T> in, final Function<T, ? extends Iterable<? extends T>> dependency) {

        final Set<T> seen = new HashSet<>();
        for (final T current : in) {
            for (final T dep : dependency.apply(current)) {
                if (!seen.contains(dep)) {
                    return false;
                }
            }
            seen.add(current);
        }
        return true;
    }

    // FIXME: must make stable
    public static <T> List<T> stableSort(final Iterable<? extends T> in, final Function<T, ? extends Iterable<? extends T>> dependency) {

        return sort(in, dependency);
    }

    // FIXME: fix to use a stack rather than recursion
    public static <T> List<T> sort(final Iterable<? extends T> in, final Function<T, ? extends Iterable<? extends T>> dependency) {

        final Set<T> seen = new HashSet<>();

        final LinkedList<T> stack = new LinkedList<>();

        for (final T current : in) {
            if (!seen.contains(current)) {
                sortUtil(current, dependency, seen, stack);
            }
        }

        Collections.reverse(stack);
        return stack;
    }

    private static <T> void sortUtil(final T current, final Function<T, ? extends Iterable<? extends T>> dependency, final Set<T> seen, final LinkedList<T> stack) {

        seen.add(current);

        for (final T next : dependency.apply(current)) {
            if (!seen.contains(next)) {
                sortUtil(next, dependency, seen, stack);
            }
        }

        stack.remove(current);
        stack.push(current);
    }
}

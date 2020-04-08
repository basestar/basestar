package io.basestar.expression.methods;

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

import com.google.common.collect.Lists;
import io.basestar.expression.function.Lambda;
import io.basestar.expression.type.Values;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

public abstract class CollectionMethods<T extends Collection<?>> {

    public int size(final T target) {

        return target.size();
    }

    public boolean isEmpty(final T target) {

        return target.isEmpty();
    }

    public boolean contains(final T target, final Object o) {

        return target.stream().anyMatch(v -> Values.equals(v, o));
    }

    public boolean containsAll(final T target, final Collection<?> os) {

        return os.stream().allMatch(o -> target.stream().anyMatch(v -> Values.equals(v, o)));
    }

    public boolean containsAny(final T target, final Collection<?> os) {

        return os.stream().anyMatch(o -> target.stream().anyMatch(v -> Values.equals(v, o)));
    }

    public boolean anyMatch(final T target, final Lambda.Callable fn) {

        return target.stream().anyMatch(v -> Values.isTruthy(fn.call(v)));
    }

    public boolean allMatch(final T target, final Lambda.Callable fn) {

        return target.stream().anyMatch(v -> Values.isTruthy(fn.call(v)));
    }

    public T map(final T target, final Lambda.Callable fn) {

        return collect(target.stream().map(fn::call));
    }

    public T flatMap(final T target, final Lambda.Callable fn) {

        return collect(target.stream().map(fn::call)
                .map(vs -> {
                    if(vs instanceof Collection<?>) {
                        return ((Collection) vs).stream();
                    } else {
                        throw new IllegalStateException();
                    }
                }));
    }

    @SuppressWarnings("unchecked")
    public Object reduce(final T target, final Lambda.Callable fn) {

        return ((Collection<Object>)target).stream().reduce(fn::call).orElse(null);
    }

    public Object reduce(final T target, final Object initial, final Lambda.Callable fn) {

        return Stream.concat(Stream.of(initial), target.stream())
                .reduce(fn::call).orElseThrow(IllegalStateException::new);
    }

    protected abstract T collect(final Stream<?> stream);


    @SuppressWarnings("unchecked")
    public List<?> sort(final T target) {

        final List<Comparable<Object>> copy = Lists.newArrayList((Collection<Comparable<Object>>)target);
        copy.sort(Comparator.naturalOrder());
        return copy;
    }

    @SuppressWarnings("unchecked")
    public List<?> sort(final T target, final Lambda.Callable fn) {

        final List<Comparable<Object>> copy = Lists.newArrayList((Collection<Comparable<Object>>)target);
        copy.sort((Comparator<Object>) (o1, o2) -> ((Number)fn.call(o1, o2)).intValue());
        return copy;
    }
}

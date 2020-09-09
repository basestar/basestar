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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

public class Nullsafe {

    @Nonnull
    public static <T> T require(@Nullable final T v) {

        return Objects.requireNonNull(v);
    }

    @Nonnull
    public static <T> T orDefault(@Nullable final T v, @Nonnull final Supplier<T> or) {

        return v == null ? Objects.requireNonNull(or.get()) : v;
    }

    @Nonnull
    public static <T> T orDefault(@Nullable final T v, @Nonnull final T or) {

        return v == null ? Objects.requireNonNull(or) : v;
    }

    @Nonnull
    public static String orDefault(@Nullable final String s) {

        return s == null ? "" : s;
    }

    @Nonnull
    public static Boolean orDefault(@Nullable final Boolean n) {

        return n == null ? Boolean.FALSE : n;
    }

    @Nonnull
    public static Integer orDefault(@Nullable final Integer n) {

        return n == null ? Integer.valueOf(0) : n;
    }

    @Nonnull
    public static Long orDefault(@Nullable final Long n) {

        return n == null ? Long.valueOf(0) : n;
    }

    @Nonnull
    public static Float orDefault(@Nullable final Float n) {

        return n == null ? Float.valueOf(0) : n;
    }

    @Nonnull
    public static Double orDefault(@Nullable final Double n) {

        return n == null ? Double.valueOf(0) : n;
    }

    @Nonnull
    public static <K, V> Map<K, V> orDefault(@Nullable final Map<K, V> m) {

        return m == null ? Collections.emptyMap() : m;
    }

    @Nonnull
    public static <V> Iterable<V> orDefault(@Nullable final Iterable<V> i) {

        return i == null ? Collections.emptyList() : i;
    }

    @Nonnull
    public static <V> Collection<V> orDefault(@Nullable final Collection<V> c) {

        return c == null ? Collections.emptyList() : c;
    }

    @Nonnull
    public static <V> List<V> orDefault(@Nullable final List<V> l) {
        
        return l == null ? Collections.emptyList() : l;
    }

    @Nonnull
    public static <V> Set<V> orDefault(@Nullable final Set<V> s) {

        return s == null ? Collections.emptySet() : s;
    }

    @Nonnull
    public static <K, V> Map<K, V> immutableCopyPut(@Nullable final Map<K, V> m, final K k, final V v) {

        if(m == null) {
            return Collections.singletonMap(k, v);
        } else {
            final HashMap<K, V> copy = new HashMap<>(m);
            copy.put(k, v);
            return Collections.unmodifiableMap(copy);
        }
    }

    @Nonnull
    public static <V> List<V> immutableCopyAdd(@Nullable final List<V> l, final V v) {

        if(l == null) {
            return Collections.singletonList(v);
        } else {
            final ArrayList<V> copy = new ArrayList<>(l);
            copy.add(v);
            return Collections.unmodifiableList(copy);
        }
    }

    @Nonnull
    public static <V> Set<V> immutableCopyAdd(@Nullable final Set<V> s, final V v) {

        if(s == null) {
            return Collections.singleton(v);
        } else {
            final HashSet<V> copy = new HashSet<>(s);
            copy.add(v);
            return Collections.unmodifiableSet(copy);
        }
    }

    @Nonnull
    public static <V> List<V> immutableCopy(@Nullable final List<? extends V> l) {

        return l == null ? Collections.emptyList() : Collections.unmodifiableList(new ArrayList<>(l));
    }

    @Nonnull
    public static <V> Set<V> immutableCopy(@Nullable final Set<? extends V> s) {

        return s == null ? Collections.emptySet() : Collections.unmodifiableSet(new HashSet<>(s));
    }

    @Nonnull
    public static <V extends Comparable<V>> SortedSet<V> immutableSortedCopy(@Nullable final Set<? extends V> s) {

        return s == null ? Collections.emptySortedSet() : Collections.unmodifiableSortedSet(new TreeSet<>(s));
    }

    @Nonnull
    public static <K, V> Map<K, V> immutableCopy(@Nullable final Map<? extends K, ? extends V> m) {

        return m == null ? Collections.emptyMap() : Collections.unmodifiableMap(new HashMap<>(m));
    }

    @Nonnull
    public static <K extends Comparable<K>, V> SortedMap<K, V> immutableCopy(@Nullable final SortedMap<? extends K, ? extends V> m) {

        return immutableSortedCopy(m);
    }

    @Nonnull
    public static <K extends Comparable<K>, V> SortedMap<K, V> immutableSortedCopy(@Nullable final Map<? extends K, ? extends V> m) {

        return m == null ? Collections.emptySortedMap() : Collections.unmodifiableSortedMap(new TreeMap<>(m));
    }

    @Nonnull
    public static <K extends Comparable<K>, V1, V2> SortedMap<K, V2> immutableSortedCopy(@Nullable final Map<? extends K, ? extends V1> m, final BiFunction<? super K, ? super V1, ? extends V2> transform) {

        if(m == null) {
            return Collections.emptySortedMap();
        } else {
            final SortedMap<K, V2> result = new TreeMap<>();
            m.forEach((k, v) -> result.put(k, transform.apply(k, v)));
            return Collections.unmodifiableSortedMap(result);
        }
    }

    @Nullable
    public static <T, U> U map(@Nullable final T v, final Function<T, U> fn) {

        return mapOrDefault(v, fn, null);
    }

    @Nullable
    public static <T, U> U mapOrDefault(@Nullable final T v, final Function<T, U> fn, final U or) {

        return v == null ? or : fn.apply(v);
    }
}

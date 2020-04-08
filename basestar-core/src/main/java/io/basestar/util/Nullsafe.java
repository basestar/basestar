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

public class Nullsafe {

    @Nonnull
    public static <T> T of(@Nullable final T v, @Nonnull final T or) {

        return v == null ? or : v;
    }

    @Nonnull
    public static <T> T of(@Nullable final T v) {

        if(v == null) {
            throw new NullPointerException();
        } else {
            return v;
        }
    }

    @Nonnull
    public static String of(@Nullable final String s) {

        return s == null ? "" : s;
    }

    @Nonnull
    public static Integer of(@Nullable final Integer n) {

        return n == null ? Integer.valueOf(0) : n;
    }

    @Nonnull
    public static Long of(@Nullable final Long n) {

        return n == null ? Long.valueOf(0) : n;
    }

    @Nonnull
    public static Float of(@Nullable final Float n) {

        return n == null ? Float.valueOf(0) : n;
    }

    @Nonnull
    public static Double of(@Nullable final Double n) {

        return n == null ? Double.valueOf(0) : n;
    }

    @Nonnull
    public static <K, V> Map<K, V> of(@Nullable final Map<K, V> m) {

        return m == null ? Collections.emptyMap() : m;
    }

    @Nonnull
    public static <V> Iterable<V> of(@Nullable final Iterable<V> i) {

        return i == null ? Collections.emptyList() : i;
    }

    @Nonnull
    public static <V> Collection<V> of(@Nullable final Collection<V> c) {

        return c == null ? Collections.emptyList() : c;
    }

    @Nonnull
    public static <V> List<V> of(@Nullable final List<V> l) {
        
        return l == null ? Collections.emptyList() : l;
    }

    @Nonnull
    public static <V> Set<V> of(@Nullable final Set<V> s) {

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
    public static <V> List<V> immutableCopy(@Nullable final List<V> l) {

        return l == null ? Collections.emptyList() : Collections.unmodifiableList(new ArrayList<>(l));
    }

    @Nonnull
    public static <V> Set<V> immutableCopy(@Nullable final Set<V> s) {

        return s == null ? Collections.emptySet() : Collections.unmodifiableSet(new HashSet<>(s));
    }

    @Nonnull
    public static <V> SortedSet<V> immutableSortedCopy(@Nullable final SortedSet<V> s) {

        return immutableSortedCopy(s);
    }

    @Nonnull
    public static <V> SortedSet<V> immutableSortedCopy(@Nullable final Set<V> s) {

        return s == null ? Collections.emptySortedSet() : Collections.unmodifiableSortedSet(new TreeSet<>(s));
    }

    @Nonnull
    public static <K, V> Map<K, V> immutableCopy(@Nullable final Map<K, V> m) {

        return m == null ? Collections.emptyMap() : Collections.unmodifiableMap(new HashMap<>(m));
    }

    @Nonnull
    public static <K, V> SortedMap<K, V> immutableCopy(@Nullable final SortedMap<K, V> m) {

        return immutableSortedCopy(m);
    }

    @Nonnull
    public static <K, V> SortedMap<K, V> immutableSortedCopy(@Nullable final Map<K, V> m) {

        return m == null ? Collections.emptySortedMap() : Collections.unmodifiableSortedMap(new TreeMap<>(m));
    }
}

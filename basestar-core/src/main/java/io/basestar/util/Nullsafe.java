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
import java.util.function.Function;
import java.util.function.Supplier;

public class Nullsafe {

    private Nullsafe() {

    }

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

    @Nullable
    public static <T, U> U map(@Nullable final T v, final Function<T, U> fn) {

        return v == null ? null : fn.apply(v);
    }

    @Nonnull
    public static <T, U> U mapOrDefault(@Nullable final T v, final Function<T, U> fn, @Nonnull final U or) {

        return v == null ? or : fn.apply(v);
    }
}

package io.basestar.util;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;

public class Immutable {

    private Immutable() {

    }

    public static <T> List<T> list() {

        return Collections.emptyList();
    }

    public static <T> List<T> list(final T value) {

        return Collections.singletonList(value);
    }

    public static <T> Set<T> set() {

        return Collections.emptySet();
    }

    public static <T> Set<T> set(final T value) {

        return Collections.singleton(value);
    }

    public static <K, V> Map<K, V> map() {

        return Collections.emptyMap();
    }

    public static <K, V> Map<K, V> map(final K key, final V value) {

        return Collections.singletonMap(key, value);
    }

    public static <K, V> Map<K, V> map(final K k1, final V v1, final K k2, final V v2) {

        final Map<K, V> map = new HashMap<>();
        map.put(k1, v1);
        map.put(k2, v2);
        return Collections.unmodifiableMap(map);
    }

    @Nonnull
    public static <K, V> Map<K, V> put(@Nullable final Map<K, V> m, final K k, final V v) {

        if (m == null || m.isEmpty()) {
            return Collections.singletonMap(k, v);
        } else {
            final HashMap<K, V> copy = new HashMap<>(m);
            copy.put(k, v);
            return Collections.unmodifiableMap(copy);
        }
    }

    @Nonnull
    public static <V> List<V> add(@Nullable final List<V> l, final V v) {

        if (l == null || l.isEmpty()) {
            return Collections.singletonList(v);
        } else {
            final List<V> copy = new ArrayList<>(l);
            copy.add(v);
            return Collections.unmodifiableList(copy);
        }
    }

    @Nonnull
    public static <V> List<V> addAll(@Nullable final List<V> l, final Collection<? extends V> vs) {

        if (l == null || l.isEmpty()) {
            if (vs == null || vs.isEmpty()) {
                return Collections.emptyList();
            } else {
                final List<V> copy = new ArrayList<>(vs);
                return Collections.unmodifiableList(copy);
            }
        } else {
            final List<V> copy = new ArrayList<>(l);
            if (vs != null) {
                copy.addAll(vs);
            }
            return Collections.unmodifiableList(copy);
        }
    }

    @Nonnull
    public static <V> Set<V> add(@Nullable final Set<V> s, final V v) {

        if (s == null || s.isEmpty()) {
            return Collections.singleton(v);
        } else {
            final Set<V> copy = new HashSet<>(s);
            copy.add(v);
            return Collections.unmodifiableSet(copy);
        }
    }

    public static <K extends Comparable<K>, V, V2 extends V> SortedMap<K, V> sortedMerge(final Map<K, V> m, final Map<K, V2> vs, final BiFunction<V, V2, V> merge) {

        return sortedMap(merge(m, vs, merge));
    }

    @Nonnull
    public static <V> Set<V> addAll(@Nullable final Set<V> s, @Nullable final Collection<? extends V> vs) {

        if (s == null || s.isEmpty()) {
            if (vs == null) {
                return Collections.emptySet();
            } else {
                final Set<V> copy = new HashSet<>(vs);
                return Collections.unmodifiableSet(copy);
            }
        } else {
            final Set<V> copy = new HashSet<>(s);
            if (vs != null) {
                copy.addAll(vs);
            }
            return Collections.unmodifiableSet(copy);
        }
    }

    @Nonnull
    public static <V> Set<V> removeAll(@Nullable final Set<V> s, @Nullable final Collection<? extends V> vs) {

        if (s == null || s.isEmpty()) {
            return Collections.emptySet();
        } else {
            final Set<V> copy = new HashSet<>(s);
            if (vs != null) {
                copy.removeAll(vs);
            }
            return Collections.unmodifiableSet(copy);
        }
    }

    @Nonnull
    public static <V extends Comparable<V>> SortedSet<V> sortedAddAll(@Nullable final Set<V> s, @Nullable final Collection<? extends V> vs) {

        return sortedSet(addAll(s, vs));
    }

    @Nonnull
    public static <V> List<V> list(@Nullable final Collection<? extends V> l) {

        return l == null || l.isEmpty() ? Collections.emptyList() : Collections.unmodifiableList(new ArrayList<>(l));
    }

    @Nonnull
    public static <V> Set<V> set(@Nullable final Collection<? extends V> s) {

        return s == null || s.isEmpty() ? Collections.emptySet() : Collections.unmodifiableSet(new HashSet<>(s));
    }

    @Nonnull
    public static <V extends Comparable<V>> SortedSet<V> sortedSet(@Nullable final Collection<? extends V> s) {

        return s == null || s.isEmpty() ? Collections.emptySortedSet() : Collections.unmodifiableSortedSet(new TreeSet<>(s));
    }

    @Nonnull
    public static <K, V> Map<K, V> map(@Nullable final Map<? extends K, ? extends V> m) {

        return m == null || m.isEmpty() ? Collections.emptyMap() : Collections.unmodifiableMap(new HashMap<>(m));
    }

    @Nonnull
    public static <K extends Comparable<K>, V> SortedMap<K, V> sortedMap(@Nullable final Map<? extends K, ? extends V> m) {

        return navigableMap(m);
    }

    @Nonnull
    public static <K extends Comparable<K>, V> NavigableMap<K, V> navigableMap(@Nullable final Map<? extends K, ? extends V> m) {

        return m == null || m.isEmpty() ? Collections.emptyNavigableMap() : Collections.unmodifiableNavigableMap(new TreeMap<>(m));
    }

    public static <K, V> Map<K, V> putAll(final Map<K, V> m, final Map<K, ? extends V> vs) {

        return merge(m, vs, (v1, v2) -> v2);
    }

    public static <K, V, V2 extends V> Map<K, V> merge(final Map<K, V> m, final Map<K, V2> vs, final BiFunction<V, V2, V> merge) {

        final Map<K, V> result = new HashMap<>();
        if (m != null) {
            result.putAll(m);
        }
        if (vs != null) {
            vs.forEach((k, v2) -> {
                final V v1 = result.get(k);
                if (v1 != null) {
                    result.put(k, merge.apply(v1, v2));
                } else {
                    result.put(k, v2);
                }
            });
        }
        return result;
    }

    @Nonnull
    public static <V1, V2> Collection<V2> transform(@Nullable final Collection<? extends V1> l, final Function<? super V1, ? extends V2> transform) {

        return transformList(l, transform);
    }

    @Nonnull
    public static <V1, V2> List<V2> transform(@Nullable final List<? extends V1> l, final Function<? super V1, ? extends V2> transform) {

        return transformList(l, transform);
    }

    @Nonnull
    public static <V1, V2> Set<V2> transform(@Nullable final Set<? extends V1> l, final Function<? super V1, ? extends V2> transform) {

        return transformSet(l, transform);
    }

    @Nonnull
    public static <V1, V2> List<V2> transformList(@Nullable final Collection<? extends V1> l, final Function<? super V1, ? extends V2> transform) {

        if (l == null || l.isEmpty()) {
            return Collections.emptyList();
        } else {
            final List<V2> copy = new ArrayList<>();
            l.forEach(v1 -> copy.add(transform.apply(v1)));
            return Collections.unmodifiableList(copy);
        }
    }

    @Nonnull
    public static <V1, V2> Set<V2> transformSet(@Nullable final Collection<? extends V1> l, final Function<? super V1, ? extends V2> transform) {

        if (l == null || l.isEmpty()) {
            return Collections.emptySet();
        } else {
            final Set<V2> copy = new HashSet<>();
            l.forEach(v1 -> copy.add(transform.apply(v1)));
            return Collections.unmodifiableSet(copy);
        }
    }

    @Nonnull
    public static <K, V1, V2> Map<K, V2> transformValues(@Nullable final Map<? extends K, ? extends V1> m, final BiFunction<? super K, ? super V1, ? extends V2> transform) {

        if (m == null || m.isEmpty()) {
            return Collections.emptyMap();
        } else {
            final Map<K, V2> result = new HashMap<>();
            m.forEach((k, v) -> result.put(k, transform.apply(k, v)));
            return Collections.unmodifiableMap(result);
        }
    }

    @Nonnull
    public static <K extends Comparable<K>, V1, V2> SortedMap<K, V2> transformValues(@Nullable final SortedMap<? extends K, ? extends V1> m, final BiFunction<? super K, ? super V1, ? extends V2> transform) {

        return transformValuesSorted(m, transform);
    }

    @Nonnull
    public static <K extends Comparable<K>, V1, V2> SortedMap<K, V2> transformValuesSorted(@Nullable final Map<? extends K, ? extends V1> m, final BiFunction<? super K, ? super V1, ? extends V2> transform) {

        if (m == null || m.isEmpty()) {
            return Collections.emptySortedMap();
        } else {
            final SortedMap<K, V2> result = new TreeMap<>();
            m.forEach((k, v) -> result.put(k, transform.apply(k, v)));
            return Collections.unmodifiableSortedMap(result);
        }
    }

    @SafeVarargs
    public static <K, V> Map<K, V> map(final Map.Entry<K, V>... entries) {

        final Map<K, V> result = new HashMap<>();
        Arrays.stream(entries).forEach(e -> result.put(e.getKey(), e.getValue()));
        return Collections.unmodifiableMap(result);
    }

    public static <K, V> Map.Entry<K, V> entry(final K k, final V v) {

        return new AbstractMap.SimpleImmutableEntry<>(k, v);
    }
}

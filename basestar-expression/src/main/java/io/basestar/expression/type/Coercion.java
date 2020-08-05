package io.basestar.expression.type;

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

import com.google.common.io.BaseEncoding;
import io.basestar.util.ISO8601;

import java.lang.reflect.Modifier;
import java.time.Instant;
import java.time.LocalDate;
import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

// FIXME: should be used in all Use<?> create methods

public class Coercion {

    public static Boolean toBoolean(final Object source) {

        if(source == null) {
            return false;
        } else if(source instanceof Boolean) {
            return (Boolean) source;
        } else if(source instanceof Number) {
            if(source instanceof Float || source instanceof Double) {
                return ((Number) source).doubleValue() != 0D;
            } else {
                return ((Number) source).longValue() != 0L;
            }
        } else if(source instanceof String) {
            return Boolean.valueOf((String)source);
        } else {
            throw new IllegalStateException();
        }
    }

    public static Byte toByte(final Object source) {

        if(source == null) {
            return null;
        } else if(source instanceof Number) {
            return ((Number) source).byteValue();
        } else if(source instanceof String) {
            return Byte.parseByte((String)source);
        } else {
            throw new IllegalStateException();
        }
    }

    public static Short toShort(final Object source) {

        if(source == null) {
            return null;
        } else if(source instanceof Number) {
            return ((Number) source).shortValue();
        } else if(source instanceof String) {
            return Short.parseShort((String)source);
        } else {
            throw new IllegalStateException();
        }
    }

    public static Integer toInteger(final Object source) {

        if(source == null) {
            return null;
        } else if(source instanceof Number) {
            return ((Number) source).intValue();
        } else if(source instanceof String) {
            return Integer.parseInt((String)source);
        } else {
            throw new IllegalStateException();
        }
    }

    public static Long toLong(final Object source) {

        if(source == null) {
            return null;
        } else if(source instanceof Number) {
            return ((Number) source).longValue();
        } else if(source instanceof String) {
            return Long.parseLong((String)source);
        } else {
            throw new IllegalStateException();
        }
    }

    public static Float toFloat(final Object source) {

        if(source == null) {
            return null;
        } else if(source instanceof Number) {
            return ((Number) source).floatValue();
        } else if(source instanceof String) {
            return Float.parseFloat((String)source);
        } else {
            throw new IllegalStateException();
        }
    }

    public static Double toDouble(final Object source) {

        if(source == null) {
            return null;
        } else if(source instanceof Number) {
            return ((Number) source).doubleValue();
        } else if(source instanceof String) {
            return Double.parseDouble((String)source);
        } else {
            throw new IllegalStateException();
        }
    }

    public static String toString(final Object source) {

        if(source == null) {
            return null;
        } else if(source instanceof byte[]) {
            return BaseEncoding.base64().encode((byte[])source);
        } else {
            return source.toString();
        }
    }

    public static LocalDate toDate(final Object source) {

        if(source == null) {
            return null;
        } else {
            return ISO8601.toDate(source);
        }
    }

    public static Instant toDateTime(final Object source) {

        if(source == null) {
            return null;
        } else {
            return ISO8601.toDateTime(source);
        }
    }

    public static <V> List<V> toList(final Object source, final Function<Object, V> value) {

        return toCollection(source, ArrayList::new, value);
    }

    public static <V> Set<V> toSet(final Object source, final Function<Object, V> value) {

        return toCollection(source, HashSet::new, value);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public static <V> List<V> toList(final Object source, final Class<? extends List> type, final Function<Object, V> value) {

        final Supplier<List<V>> supplier;
        if(Modifier.isAbstract(type.getModifiers())) {
            supplier = ArrayList::new;
        } else {
            supplier = () -> {
                try {
                    return type.newInstance();
                } catch (final InstantiationException | IllegalAccessException e) {
                    throw new IllegalStateException(e);
                }
            };
        }
        return toCollection(source, supplier, value);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public static <V> Set<V> toSet(final Object source, final Class<? extends Set> type, final Function<Object, V> value) {

        final Supplier<Set<V>> supplier;
        if(Modifier.isAbstract(type.getModifiers())) {
            if(SortedSet.class.isAssignableFrom(type)) {
                supplier = TreeSet::new;
            } else {
                supplier = HashSet::new;
            }
        } else {
            supplier = () -> {
                try {
                    return type.newInstance();
                } catch (final InstantiationException | IllegalAccessException e) {
                    throw new IllegalStateException(e);
                }
            };
        }
        return toCollection(source, supplier, value);
    }

    public static <C extends Collection<V>, V> C toCollection(final Object source, final Supplier<C> supplier, final Function<Object, V> value) {

        if(source == null) {
            return null;
        } else if(source instanceof Collection<?>) {
            return ((Collection<?>) source).stream().map(value).collect(Collectors.toCollection(supplier));
        } else {
            throw new IllegalStateException();
        }
    }

    public static <K, V> Map<K, V> toMap(final Object source, final Function<Object, K> key, final Function<Object, V> value) {

        return toMap(source, HashMap::new, key, value);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public static <K, V> Map<K, V> toMap(final Object source, final Class<? extends Map> type, final Function<Object, K> key, final Function<Object, V> value) {

        final Supplier<Map<K, V>> supplier;
        if(Modifier.isAbstract(type.getModifiers())) {
            if(SortedMap.class.isAssignableFrom(type)) {
                supplier = TreeMap::new;
            } else {
                supplier = HashMap::new;
            }
        } else {
            supplier = () -> {
                try {
                    return type.newInstance();
                } catch (final InstantiationException | IllegalAccessException e) {
                    throw new IllegalStateException(e);
                }
            };
        }
        return toMap(source, supplier, key, value);
    }

    public static <K, V> Map<K, V> toMap(final Object source, final Supplier<Map<K, V>> supplier, final Function<Object, K> key, final Function<Object, V> value) {

        if(source == null) {
            return null;
        } else if(source instanceof Map<?, ?>) {
            final Map<K, V> result = supplier.get();
            ((Map<?, ?>) source).forEach((k, v) -> {
                result.put(key.apply(k), value.apply(v));
            });
            return result;
        } else {
            throw new IllegalStateException();
        }
    }

    public static byte[] toBinary(final Object source) {

        if(source == null) {
            return null;
        } else if(source instanceof byte[]) {
            return (byte[])source;
        } else if(source instanceof String) {
            return BaseEncoding.base64().decode((String)source);
        } else {
            throw new IllegalStateException();
        }
    }
}

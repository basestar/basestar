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
import io.basestar.expression.exception.TypeConversionException;
import io.basestar.util.Bytes;
import io.basestar.util.ISO8601;

import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.temporal.TemporalAccessor;
import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class Coercion {

    private Coercion() {

    }

    public static boolean isTruthy(final Object value) {

        if (value == null) {
            return false;
        } else if (value instanceof Boolean) {
            return (Boolean) value;
        } else if (value instanceof Number) {
            final Number number = (Number) value;
            if (Numbers.isInteger(number)) {
                return number.intValue() != 0;
            } else {
                return number.floatValue() != 0.0f;
            }
        } else if (value instanceof CharSequence) {
            final String str = value.toString();
            return !(str.isEmpty() || str.equalsIgnoreCase("false"));
        } else if (value instanceof Collection) {
            return !((Collection<?>) value).isEmpty();
        } else if (value instanceof Map) {
            return !((Map<?, ?>) value).isEmpty();
        } else {
            throw new TypeConversionException(Boolean.class, value);
        }
    }

    public static Boolean toBoolean(final Object value) {

        if (value == null) {
            return null;
        } else if (value instanceof Boolean) {
            return (Boolean) value;
        } else if (value instanceof Number) {
            if (value instanceof Float || value instanceof Double) {
                return ((Number) value).doubleValue() != 0D;
            } else {
                return ((Number) value).longValue() != 0L;
            }
        } else if (value instanceof CharSequence) {
            final String str = value.toString();
            return !(str.isEmpty() || str.equalsIgnoreCase("false"));
        } else {
            throw new TypeConversionException(Boolean.class, value);
        }
    }

    public static Long toInteger(final Object value) {

        if (value == null) {
            return null;
        } else if (value instanceof Boolean) {
            return ((Boolean) value) ? 1L : 0L;
        } else if (value instanceof Number) {
            return ((Number) value).longValue();
        } else if (value instanceof CharSequence) {
            final String str = value.toString();
            try {
                return Long.parseLong(str);
            } catch (final NumberFormatException e) {
                // ignore, try to parse as double and convert
            }
            try {
                return ((Double) Double.parseDouble(str)).longValue();
            } catch (final NumberFormatException e) {
                throw new TypeConversionException(Long.class, value);
            }
        } else if (value instanceof LocalDate) {
            return ISO8601.toMillis((LocalDate) value);
        } else if (value instanceof Instant) {
            return ISO8601.toMillis((Instant) value);
        } else if (value instanceof Date) {
            return ISO8601.toMillis((Date) value);
        } else {
            throw new TypeConversionException(Long.class, value);
        }
    }

    public static Double toFloat(final Object value) {

        if (value == null) {
            return null;
        } else if (value instanceof Boolean) {
            return ((Boolean) value) ? 1.0 : 0.0;
        } else if (value instanceof Number) {
            return ((Number) value).doubleValue();
        } else if (value instanceof CharSequence) {
            try {
                return Double.parseDouble(value.toString());
            } catch (final NumberFormatException e) {
                throw new TypeConversionException(Double.class, value);
            }
        } else if (value instanceof LocalDate) {
            return (double) ISO8601.toMillis((LocalDate) value);
        } else if (value instanceof Instant) {
            return (double) ISO8601.toMillis((Instant) value);
        } else if (value instanceof Date) {
            return (double) ISO8601.toMillis((Date) value);
        } else {
            throw new TypeConversionException(Double.class, value);
        }
    }

    public static BigDecimal toDecimal(final Object value) {

        if (value == null) {
            return null;
        } else if (value instanceof Boolean) {
            return ((Boolean) value) ? BigDecimal.ONE : BigDecimal.ZERO;
        } else if (value instanceof Number) {
            final Number number = (Number) value;
            if (Numbers.isInteger(number)) {
                return BigDecimal.valueOf(number.longValue());
            } else {
                return BigDecimal.valueOf(number.doubleValue());
            }
        } else if (value instanceof CharSequence) {
            try {
                return new BigDecimal(value.toString());
            } catch (final NumberFormatException e) {
                throw new TypeConversionException(Double.class, value);
            }
        } else if (value instanceof LocalDate) {
            return BigDecimal.valueOf(ISO8601.toMillis((LocalDate) value));
        } else if (value instanceof Instant) {
            return BigDecimal.valueOf(ISO8601.toMillis((Instant) value));
        } else if (value instanceof Date) {
            return BigDecimal.valueOf(ISO8601.toMillis((Date) value));
        } else {
            throw new TypeConversionException(BigDecimal.class, value);
        }
    }

    public static String toString(final Object value) {

        if (value == null) {
            return null;
        } else if (value instanceof Boolean || value instanceof Number) {
            return value.toString();
        } else if (value instanceof TemporalAccessor) {
            return ISO8601.toString((TemporalAccessor) value);
        } else if (value instanceof Date) {
            return ISO8601.toString((Date) value);
        } else if (value instanceof CharSequence) {
            return value.toString();
        } else if (value instanceof Bytes) {
            return value.toString();
        } else if (value instanceof byte[]) {
            return BaseEncoding.base64().encode((byte[]) value);
        } else {
            throw new TypeConversionException(String.class, value);
        }
    }

    public static LocalDate toDate(final Object source) {

        if (source == null) {
            return null;
        } else {
            return ISO8601.toDate(source);
        }
    }

    public static Instant toDateTime(final Object source) {

        if (source == null) {
            return null;
        } else {
            return ISO8601.toDateTime(source);
        }
    }

    public static <V> List<V> toList(final Object source, final Function<Object, V> value) {

        return toList(source, List.class, value);
    }

    public static <V> Set<V> toSet(final Object source, final Function<Object, V> value) {

        return toSet(source, Set.class, value);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public static <V> List<V> toList(final Object source, final Class<? extends List> type, final Function<Object, V> value) {

        final Supplier<List<V>> supplier;
        if (Modifier.isAbstract(type.getModifiers())) {
            supplier = ArrayList::new;
        } else {
            supplier = () -> {
                try {
                    return type.newInstance();
                } catch (final InstantiationException | IllegalAccessException e) {
                    throw new TypeConversionException(List.class, value);
                }
            };
        }
        return toCollection(source, supplier, value);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public static <V> Set<V> toSet(final Object source, final Class<? extends Set> type, final Function<Object, V> value) {

        final Supplier<Set<V>> supplier;
        if (Modifier.isAbstract(type.getModifiers())) {
            if (SortedSet.class.isAssignableFrom(type)) {
                supplier = TreeSet::new;
            } else {
                supplier = HashSet::new;
            }
        } else {
            supplier = () -> {
                try {
                    return type.newInstance();
                } catch (final InstantiationException | IllegalAccessException e) {
                    throw new TypeConversionException(List.class, value);
                }
            };
        }
        return toCollection(source, supplier, value);
    }

    public static <C extends Collection<V>, V> C toCollection(final Object source, final Supplier<C> supplier, final Function<Object, V> value) {

        if (source == null) {
            return null;
        } else if (source instanceof Collection<?>) {
            return ((Collection<?>) source).stream().map(value).collect(Collectors.toCollection(supplier));
        } else {
            throw new TypeConversionException(Collection.class, value);
        }
    }

    public static <K, V> Map<K, V> toMap(final Object source, final Function<Object, K> key, final Function<Object, V> value) {

        return toMap(source, HashMap::new, key, value);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public static <K, V> Map<K, V> toMap(final Object source, final Class<? extends Map> type, final Function<Object, K> key, final Function<Object, V> value) {

        final Supplier<Map<K, V>> supplier;
        if (Modifier.isAbstract(type.getModifiers())) {
            if (SortedMap.class.isAssignableFrom(type)) {
                supplier = TreeMap::new;
            } else {
                supplier = HashMap::new;
            }
        } else {
            supplier = () -> {
                try {
                    return type.newInstance();
                } catch (final InstantiationException | IllegalAccessException e) {
                    throw new TypeConversionException(Map.class, value);
                }
            };
        }
        return toMap(source, supplier, key, value);
    }

    public static <K, V> Map<K, V> toMap(final Object source, final Supplier<Map<K, V>> supplier, final Function<Object, K> key, final Function<Object, V> value) {

        if (source == null) {
            return null;
        } else if (source instanceof Map<?, ?>) {
            final Map<K, V> result = supplier.get();
            ((Map<?, ?>) source).forEach((k, v) -> {
                result.put(key.apply(k), value.apply(v));
            });
            return result;
        } else {
            throw new TypeConversionException(Map.class, value);
        }
    }

    public static Bytes toBinary(final Object value) {

        if (value == null) {
            return null;
        } else if (value instanceof Bytes) {
            return (Bytes)value;
        } else if (value instanceof byte[]) {
            return new Bytes((byte[]) value);
        } else if (value instanceof ByteBuffer) {
            return new Bytes(((ByteBuffer) value).array());
        } else if (value instanceof String) {
            return Bytes.fromBase64((String) value);
        } else {
            throw new TypeConversionException(Bytes.class, value);
        }
    }

    public static String className(final Object value) {

        return value == null ? "null" : className(value.getClass());
    }

    public static String className(final Class<?> cls) {

        return cls.getName();
    }
}
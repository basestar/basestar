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

import com.google.common.base.Charsets;
import com.google.common.io.BaseEncoding;
import io.basestar.expression.type.exception.TypeConversionException;
import io.basestar.expression.type.match.BinaryMatch;
import io.basestar.expression.type.match.BinaryNumberMatch;
import io.basestar.expression.type.match.UnaryMatch;
import io.basestar.util.ISO8601;
import io.basestar.util.Pair;
import io.leangen.geantyref.GenericTypeReflector;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.temporal.TemporalAccessor;
import java.util.*;
import java.util.stream.Collectors;

// FIXME: some of these methods should be superseded by methods in Coercion/Numbers

public class Values {

    public static boolean isTruthy(final Object value) {

        if(value == null) {
            return false;
        } else if(value instanceof Boolean) {
            return (Boolean)value;
        } else if(value instanceof Number) {
            final Number number = (Number)value;
            if(isInteger(number)) {
                return number.intValue() != 0;
            } else {
                return number.floatValue() != 0.0f;
            }
        } else if(value instanceof String) {
            final String str = (String)value;
            return !(str.isEmpty() || str.equalsIgnoreCase("false"));
        } else if(value instanceof Collection) {
            return ((Collection<?>)value).size() > 0;
        } else if(value instanceof Map) {
            return ((Map<?, ?>)value).size() > 0;
        } else {
            throw new IllegalStateException();
        }
    }

    public static Boolean toBoolean(final Object value) {

        if(value == null) {
            return null;
        } else if(value instanceof Boolean) {
            return (Boolean)value;
        } else if(value instanceof Number) {
            return ((Number)value).intValue() != 0;
        } else if(value instanceof String) {
            final String str = (String)value;
            return !(str.isEmpty() || str.equalsIgnoreCase("false"));
        } else {
            throw new TypeConversionException(Boolean.class, value);
        }
    }

    public static Long toInteger(final Object value) {

        if(value == null) {
            return null;
        } else if(value instanceof Boolean) {
            return ((Boolean)value) ? 1L : 0L;
        } else if(value instanceof Number) {
            return ((Number)value).longValue();
        } else if(value instanceof String) {
            try {
                return Long.parseLong((String) value);
            } catch (final NumberFormatException e) {
                throw new TypeConversionException(Long.class, value);
            }
        } else if(value instanceof LocalDate) {
            return ISO8601.toMillis((LocalDate)value);
        } else if(value instanceof Instant) {
            return ISO8601.toMillis((Instant) value);
        } else if(value instanceof Date) {
            return ISO8601.toMillis((Date)value);
        } else {
            throw new TypeConversionException(Long.class, value);
        }
    }

    public static Double toFloat(final Object value) {

        if(value == null) {
            return null;
        } else if(value instanceof Boolean) {
            return ((Boolean)value) ? 1.0 : 0.0;
        } else if(value instanceof Number) {
            return ((Number)value).doubleValue();
        } else if(value instanceof String) {
            try {
                return Double.parseDouble((String)value);
            } catch (final NumberFormatException e) {
                throw new TypeConversionException(Double.class, value);
            }
        } else if(value instanceof LocalDate) {
            return (double)ISO8601.toMillis((LocalDate)value);
        } else if(value instanceof Instant) {
            return (double)ISO8601.toMillis((Instant)value);
        } else if(value instanceof Date) {
            return (double)ISO8601.toMillis((Date)value);
        } else {
            throw new TypeConversionException(Double.class, value);
        }
    }

    public static String toString(final Object value) {

        if(value == null) {
            return null;
        } else if(value instanceof Boolean || value instanceof Number) {
            return value.toString();
        } else if(value instanceof TemporalAccessor) {
            return ISO8601.toString((TemporalAccessor)value);
        } else if(value instanceof Date) {
            return ISO8601.toString((Date)value);
        } else if(value instanceof String) {
            return (String) value;
        } else if(value instanceof byte[]) {
            return BaseEncoding.base64().encode((byte[])value);
        } else {
            throw new TypeConversionException(String.class, value);
        }
    }

    public static byte[] toBinary(final Object value) {

        if(value == null) {
            return null;
        } else if(value instanceof byte[]) {
            return (byte[])value;
        } else if(value instanceof ByteBuffer) {
            return ((ByteBuffer) value).array();
        } else if(value instanceof String) {
            return BaseEncoding.base64().decode((String)value);
        } else {
            throw new TypeConversionException(byte[].class, value);
        }
    }

    public static boolean isInteger(final Number value) {

        return !isFloat(value);
    }

    public static boolean isFloat(final Number value) {

        return value instanceof Float || value instanceof Double || value instanceof BigDecimal;
    }

    @SuppressWarnings("unchecked")
    public static int compare(final Object a, final Object b) {

        final Pair<Object, Object> pair = promote(a, b);
        final Comparable<Object> first = (Comparable<Object>)pair.getFirst();
        final Comparable<Object> second = (Comparable<Object>)pair.getSecond();
        return Objects.compare(first, second, Comparator.naturalOrder());
    }

    public static boolean equals(final Object a, final Object b) {

        return EQUALS.apply(a, b);
    }

    public static Pair<Object, Object> promote(final Object a, final Object b) {

        return PROMOTE.apply(a, b);
    }

    public static String toExpressionString(final Object value) {

        return TO_EXPRESSION_STRING.apply(value);
    }

    public static String toExpressionString(final Collection<?> args) {

        return "[" + args.stream().map(Values::toExpressionString).collect(Collectors.joining(", ")) + "]";
    }

    public static String toExpressionString(final Map<?, ?> args) {

        return "{" + args.entrySet().stream().map(v -> toExpressionString(v.getKey()) + ": " + toExpressionString(v.getValue()))
                .collect(Collectors.joining(", ")) + "}";
    }

    private static final BinaryNumberMatch<Pair<Object, Object>> NUMBER_PROMOTE = new BinaryNumberMatch.Promoting<Pair<Object, Object>>() {

        @Override
        public <U extends Number> Pair<Object, Object> defaultApplySame(final U a, final U b) {

            return Pair.of(a, b);
        }
    };

    private static final BinaryMatch<Pair<Object, Object>> PROMOTE = new BinaryMatch.Promoting<Pair<Object, Object>>() {

        @Override
        public String toString() {

            return "promote";
        }

        @Override
        public <U> Pair<Object, Object> defaultApplySame(final U a, final U b) {

            return Pair.of(a, b);
        }

        @Override
        public Pair<Object, Object> apply(final LocalDate a, final String b) {

            return Pair.of(a, ISO8601.parsePartialDate(b));
        }

        @Override
        public Pair<Object, Object> apply(final Instant a, final String b) {

            return Pair.of(a, ISO8601.parsePartialDateTime(b));
        }

        @Override
        public Pair<Object, Object> apply(final String a, final LocalDate b) {

            return Pair.of(ISO8601.parsePartialDate(a), b);
        }

        @Override
        public Pair<Object, Object> apply(final String a, final Instant b) {

            return Pair.of(ISO8601.parsePartialDateTime(a), b);
        }

        @Override
        public Pair<Object, Object> apply(final Number a, final Number b) {

            return NUMBER_PROMOTE.apply(a, b);
        }
    };

    private static final BinaryNumberMatch<Boolean> NUMBER_EQUALS = new BinaryNumberMatch.Promoting<Boolean>() {

        @Override
        public <U extends Number> Boolean defaultApplySame(final U a, final U b) {

            return Objects.equals(a, b);
        }
    };

    private static final BinaryMatch<Boolean> EQUALS = new BinaryMatch<Boolean>() {

        @Override
        public Boolean defaultApply(final Object lhs, final Object rhs) {

            /// Byte arrays are an almost-first-class type
            if(lhs instanceof byte[] && rhs instanceof byte[]) {
                return Arrays.equals((byte[])lhs, (byte[])rhs);
            } else {
                return Objects.equals(lhs, rhs);
            }
        }

        @Override
        public Boolean apply(final Number lhs, final Number rhs) {

            return NUMBER_EQUALS.apply(lhs, rhs);
        }

        @Override
        public Boolean apply(final Collection<?> lhs, final Collection<?> rhs) {

            if(lhs.size() == rhs.size()) {
                final Iterator<?> a = lhs.iterator();
                final Iterator<?> b = rhs.iterator();
                while(a.hasNext() && b.hasNext()) {
                   if(!apply(a.next(), b.next())) {
                       return false;
                   }
                }
                return true;
            } else {
                return false;
            }
        }

        @Override
        public Boolean apply(final Map<?, ?> lhs, final Map<?, ?> rhs) {

            if(lhs.size() == rhs.size()) {
                final Set<?> keys = lhs.keySet();
                if(Objects.equals(rhs.keySet(), keys)) {
                    for(final Object key : keys) {
                        if(!apply(lhs.get(key), rhs.get(key))) {
                            return false;
                        }
                    }
                    return true;
                } else {
                    return false;
                }
            } else {
                return false;
            }
        }
    };

    private static final UnaryMatch<String> TO_EXPRESSION_STRING = new UnaryMatch<String>() {

        @Override
        public String defaultApply(final Object value) {

            return Objects.toString(value);
        }

        @Override
        public String apply(final String value) {


            return "\"" + value.replaceAll("\"", "\\\\\"") + "\"";
        }

        @Override
        public String apply(final Collection<?> value) {

            return Values.toExpressionString(value);
        }

        @Override
        public String apply(final Map<?, ?> value) {

            return Values.toExpressionString(value);
        }
    };

    public static String className(final Object value) {

        return value == null ? "null" : value.getClass().getName();
    }

    public static Object defaultValue(final Type of) {

        return defaultValue(GenericTypeReflector.erase(of));
    }

    @SuppressWarnings("unchecked")
    public static <T> T defaultValue(final Class<T> of) {

        if(Boolean.class.isAssignableFrom(of) || boolean.class.isAssignableFrom(of)) {
            return (T)(Boolean)false;
        } else if(String.class.isAssignableFrom(of)) {
            return (T)"";
        } else if(Numbers.isNumberType(of)) {
            return Numbers.zero(of);
        } else if(List.class.isAssignableFrom(of)) {
            return (T)Collections.emptyList();
        } else if(Set.class.isAssignableFrom(of)) {
            return (T)Collections.emptySet();
        } else if(Map.class.isAssignableFrom(of)) {
            return (T)Collections.emptyMap();
        } else {
            throw new UnsupportedOperationException();
        }
    }

    public static byte[] binaryKey(final List<?> keys) {

        final byte T_NULL = 1;
        final byte T_FALSE = 2;
        final byte T_TRUE = 3;
        final byte T_INT = 4;
        final byte T_STRING = 5;
        final byte T_DATE = 6;
        final byte T_DATETIME = 7;
        final byte T_BYTES = 8;

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();

        try {
            for(final Object v : keys) {
                if(v == null) {
                    baos.write(T_NULL);
                } else if(v instanceof Boolean) {
                    baos.write(((Boolean)v) ? T_TRUE : T_FALSE);
                } else if(v instanceof Integer || v instanceof Long) {
                    final byte[] bytes = longBytes((Number)v);
                    baos.write(T_INT);
                    baos.write(bytes);
                } else if(v instanceof String) {
                    baos.write(T_STRING);
                    baos.write(stringBytes((String)v));
                } else if(v instanceof LocalDate) {
                    final byte[] bytes = dateBytes((LocalDate)v);
                    baos.write(T_DATE);
                    baos.write(bytes);
                } else if(v instanceof Instant) {
                    final byte[] bytes = datetimeBytes((Instant)v);
                    baos.write(T_DATETIME);
                    baos.write(bytes);
                } else if(v instanceof byte[]) {
                    baos.write(T_BYTES);
                    baos.write(((byte[]) v));
                } else {
                    throw new IllegalStateException("Cannot convert " + v.getClass() + " to binary");
                }
            }

        } catch (final IOException e) {
            throw new IllegalStateException(e);
        }

        return baos.toByteArray();
    }

    public static byte[] concat(final byte[] ... arrays) {

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();

        try {
            for(final byte[] array : arrays) {
                baos.write(array);
            }
        } catch (final IOException e) {
            throw new IllegalStateException(e);
        }

        return baos.toByteArray();
    }

    private static byte[] dateBytes(final LocalDate v) {

        return datetimeBytes(v.atStartOfDay().toInstant(ZoneOffset.UTC));
    }

    private static byte[] datetimeBytes(final Instant v) {

        return longBytes(v.toEpochMilli());
    }

    private static byte[] longBytes(final Number v) {

        final ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(v.longValue());
        return buffer.array();
    }

    private static byte[] stringBytes(final String str) {

        if(str.contains("\0")) {
            throw new IllegalStateException("String used in index cannot contain NULL byte");
        }
        return str.getBytes(Charsets.UTF_8);
    }
}

package io.basestar.expression.type;

import io.basestar.expression.type.match.BinaryMatch;
import io.basestar.expression.type.match.BinaryNumberMatch;
import io.basestar.expression.type.match.UnaryMatch;
import io.basestar.util.Pair;

import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;

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
            return ((String)value).length() > 0;
        } else if(value instanceof Collection) {
            return ((Collection)value).size() > 0;
        } else if(value instanceof Map) {
            return ((Map)value).size() > 0;
        } else {
            throw new IllegalStateException();
        }
    }

    public static boolean isInteger(final Number value) {

        return !isFloat(value);
    }

    public static boolean isFloat(final Number value) {

        return value instanceof Float || value instanceof Double || value instanceof BigDecimal;
    }

    public static Number parseNumber(final String str) {

        if(str.contains(".") || str.contains("e")) {
            return Double.parseDouble(str);
        } else {
            return Long.parseLong(str);
        }
    }

    @SuppressWarnings("unchecked")
    public static int compare(final Object a, final Object b) {

        final Pair<Object> pair = promote(a, b);
        final Comparable<Object> first = (Comparable<Object>)pair.getFirst();
        final Comparable<Object> second = (Comparable<Object>)pair.getSecond();
        return Objects.compare(first, second, Comparator.naturalOrder());
    }

    public static boolean equals(final Object a, final Object b) {

        return EQUALS.apply(a, b);
//        if(Objects.equals(a, b)) {
//            return true;
//        } else {
//            final Pair<Object> pair = promote(a, b);
//            return pair.getFirst().equals(pair.getSecond());
//        }
    }

    public static Pair<Object> promote(final Object a, final Object b) {

        return PROMOTE.apply(a, b);
    }

    public static Pair<Object> coerce(final Object a, final Object b) {

        return COERCER.apply(a, b);
    }

    public static String toString(final Object value) {

        return TO_STRING.apply(value);
    }

    public static String toString(final Collection<?> args) {

        return "[" + args.stream().map(Values::toString).collect(Collectors.joining(", ")) + "]";
    }

    public static String toString(final Map<?, ?> args) {

        return "{" + args.entrySet().stream().map(v -> toString(v.getKey()) + ": " + toString(v.getValue()))
                .collect(Collectors.joining(", ")) + "}";
    }

    private static final BinaryNumberMatch<Pair<Object>> NUMBER_PROMOTE = new BinaryNumberMatch.Promoting<Pair<Object>>() {

        @Override
        public <U extends Number> Pair<Object> defaultApplySame(final U a, final U b) {

            return Pair.of(a, b);
        }
    };

    private static final BinaryMatch<Pair<Object>> PROMOTE = new BinaryMatch.Promoting<Pair<Object>>() {

        @Override
        public String toString() {

            return "promote";
        }

        @Override
        public <U> Pair<Object> defaultApplySame(final U a, final U b) {

            return Pair.of(a, b);
        }

        @Override
        public Pair<Object> apply(final Number a, final Number b) {

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

    private static final BinaryMatch<Pair<Object>> COERCER = new BinaryMatch.Coercing<Pair<Object>>() {

        @Override
        public String toString() {

            return "coerce";
        }

        @Override
        public <U> Pair<Object> defaultApplySame(final U a, final U b) {

            return Pair.of(a, b);
        }

        @Override
        public Pair<Object> apply(final Number a, final Number b) {

            return NUMBER_PROMOTE.apply(a, b);
        }
    };

    private static final UnaryMatch<String> TO_STRING = new UnaryMatch<String>() {

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

            return Values.toString(value);
        }

        @Override
        public String apply(final Map<?, ?> value) {

            return Values.toString(value);
        }
    };

    public static String className(final Object value) {

        return value == null ? "null" : value.getClass().getName();
    }

}

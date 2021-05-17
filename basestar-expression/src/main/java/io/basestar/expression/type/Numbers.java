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


import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;

public class Numbers {

    private static final Byte BYTE_ZERO = 0;

    private static final Short SHORT_ZERO = 0;

    private static final Integer INTEGER_ZERO = 0;

    private static final Long LONG_ZERO = 0L;

    private static final Float FLOAT_ZERO = 0F;

    private static final Double DOUBLE_ZERO = 0D;

    private static final BigDecimal DECIMAL_ZERO = BigDecimal.ZERO;

    private static final boolean DECIMAL_OPTION_DEC31 = true;

    public static final RoundingMode DECIMAL_ROUNDING_MODE = RoundingMode.HALF_UP;

    public static boolean isInteger(final Number value) {

        return !isReal(value);
    }

    public static boolean isReal(final Number value) {

        return isFloat(value) || isDecimal(value);
    }

    public static boolean isFloat(final Number value) {

        return value instanceof Float || value instanceof Double;
    }

    public static boolean isDecimal(final Number value) {

        return value instanceof BigDecimal;
    }

    public static boolean isBooleanType(final Class<?> cls) {

        return Boolean.class.isAssignableFrom(cls) || boolean.class.isAssignableFrom(cls);
    }

    public static boolean isNumberType(final Class<?> cls) {

        return isIntegerType(cls) || isRealType(cls);
    }

    public static boolean isIntegerType(final Class<?> cls) {

        return byte.class.isAssignableFrom(cls) || short.class.isAssignableFrom(cls) || int.class.isAssignableFrom(cls) || long.class.isAssignableFrom(cls)
                || Byte.class.isAssignableFrom(cls) || Short.class.isAssignableFrom(cls) || Integer.class.isAssignableFrom(cls) || Long.class.isAssignableFrom(cls)
                || BigInteger.class.isAssignableFrom(cls);
    }

    public static boolean isRealType(final Class<?> cls) {

        return isFloatType(cls) || isDecimalType(cls);
    }

    public static boolean isFloatType(final Class<?> cls) {

        return float.class.isAssignableFrom(cls) || double.class.isAssignableFrom(cls)
                || Float.class.isAssignableFrom(cls) || Double.class.isAssignableFrom(cls);
    }

    public static boolean isDecimalType(final Class<?> cls) {

        return BigDecimal.class.isAssignableFrom(cls);
    }

    public static long longValue(final Number number) {

        return number.longValue();
    }

    public static double doubleValue(final Number number) {

        return number.doubleValue();
    }

    public static BigDecimal decimalValue(final Number number) {

        if (isDecimal(number)) {
            return (BigDecimal) number;
        } else if(isFloat(number)) {
            return BigDecimal.valueOf(doubleValue(number));
        } else {
            return BigDecimal.valueOf(longValue(number));
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> T zero(final Class<T> to) {

        if(byte.class.isAssignableFrom(to) || Byte.class.isAssignableFrom(to)) {
            return (T)BYTE_ZERO;
        } else if(short.class.isAssignableFrom(to) || Short.class.isAssignableFrom(to)) {
            return (T)SHORT_ZERO;
        } else if(int.class.isAssignableFrom(to) || Integer.class.isAssignableFrom(to)) {
            return (T)INTEGER_ZERO;
        } else if(long.class.isAssignableFrom(to) || Long.class.isAssignableFrom(to)) {
            return (T)LONG_ZERO;
        } else if(float.class.isAssignableFrom(to) || Float.class.isAssignableFrom(to)) {
            return (T)FLOAT_ZERO;
        } else if(double.class.isAssignableFrom(to) || Double.class.isAssignableFrom(to)) {
            return (T)DOUBLE_ZERO;
        } else if(BigDecimal.class.isAssignableFrom(to)) {
            return (T)DECIMAL_ZERO;
        } else {
            throw new IllegalStateException();
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> T coerce(final Number v, final Class<T> to) {

        if(v == null) {
            if(to.isPrimitive()) {
                return zero(to);
            } else {
                return null;
            }
        } else if(byte.class.isAssignableFrom(to) || Byte.class.isAssignableFrom(to)) {
            return (T)(Byte)v.byteValue();
        } else if(short.class.isAssignableFrom(to) || Short.class.isAssignableFrom(to)) {
            return (T)(Short)v.shortValue();
        } else if(int.class.isAssignableFrom(to) || Integer.class.isAssignableFrom(to)) {
            return (T)(Integer)v.intValue();
        } else if(long.class.isAssignableFrom(to) || Long.class.isAssignableFrom(to)) {
            return (T)(Long)v.longValue();
        } else if(float.class.isAssignableFrom(to) || Float.class.isAssignableFrom(to)) {
            return (T)(Float)v.floatValue();
        } else if(double.class.isAssignableFrom(to) || Double.class.isAssignableFrom(to)) {
            return (T)(Double)v.doubleValue();
        } else if(BigInteger.class.isAssignableFrom(to)) {
            if(v instanceof BigInteger) {
                return (T)v;
            } else if(v instanceof BigDecimal) {
                return (T)((BigDecimal) v).toBigInteger();
            } else {
                return (T)BigInteger.valueOf(v.longValue());
            }
        } else if(BigDecimal.class.isAssignableFrom(to)) {
            if(v instanceof BigDecimal) {
                return (T)v;
            } else if(v instanceof BigInteger) {
                return (T)new BigDecimal((BigInteger) v);
            } else {
                return (T)BigDecimal.valueOf(v.doubleValue());
            }
        } else {
            throw new IllegalStateException();
        }
    }
}

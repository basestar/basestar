package io.basestar.expression.methods;

import io.basestar.expression.type.Values;
import io.basestar.util.ISO8601;

import java.time.Instant;
import java.time.LocalDate;

@SuppressWarnings("unused")
public class ObjectMethods {

    public Long toBoolean(final Object value) {

        return Values.toInteger(value);
    }

    public Long toInteger(final Object value) {

        return Values.toInteger(value);
    }

    public Double toFloat(final Object value) {

        return Values.toFloat(value);
    }

    public String toString(final Object value) {

        return Values.toString(value);
    }

    public byte[] toBinary(final Object value) {

        return Values.toBinary(value);
    }

    public LocalDate toDate(final Object value) {

        return ISO8601.toDate(value);
    }

    public Instant toDatetime(final Object value) {

        return ISO8601.toDateTime(value);
    }
}

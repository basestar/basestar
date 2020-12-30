package io.basestar.expression.methods;

import io.basestar.expression.type.Coercion;

import java.io.Serializable;
import java.time.Instant;
import java.time.LocalDate;

@SuppressWarnings("unused")
public class ObjectMethods implements Serializable {

    public Boolean toBoolean(final Object value) {

        return Coercion.toBoolean(value);
    }

    public Long toInteger(final Object value) {

        return Coercion.toInteger(value);
    }

    public Double toNumber(final Object value) {

        return Coercion.toFloat(value);
    }

    public String toString(final Object value) {

        return Coercion.toString(value);
    }

    public byte[] toBinary(final Object value) {

        return Coercion.toBinary(value);
    }

    public LocalDate toDate(final Object value) {

        return Coercion.toDate(value);
    }

    public Instant toDatetime(final Object value) {

        return Coercion.toDateTime(value);
    }
}

package io.basestar.expression.methods;

import io.basestar.expression.type.Coercion;
import io.basestar.util.Bytes;

import java.io.Serializable;
import java.time.Instant;
import java.time.LocalDate;

@SuppressWarnings("unused")
public class ObjectMethods implements Serializable {

    public Boolean toboolean(final Object value) {

        return Coercion.toBoolean(value);
    }

    public Long tointeger(final Object value) {

        return Coercion.toInteger(value);
    }

    public Double tonumber(final Object value) {

        return Coercion.toFloat(value);
    }

    public String tostring(final Object value) {

        return Coercion.toString(value);
    }

    public Bytes tobinary(final Object value) {

        return Coercion.toBinary(value);
    }

    public LocalDate todate(final Object value) {

        return Coercion.toDate(value);
    }

    public Instant todatetime(final Object value) {

        return Coercion.toDateTime(value);
    }
}

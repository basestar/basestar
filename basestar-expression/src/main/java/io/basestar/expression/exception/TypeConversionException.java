package io.basestar.expression.exception;

import java.lang.reflect.Type;

public class TypeConversionException extends RuntimeException {

    public TypeConversionException(final Type type, final Object value) {

        super("Cannot convert " + value + " to " + type.getTypeName());
    }
}

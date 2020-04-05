package io.basestar.schema.exception;

import io.basestar.exception.ExceptionMetadata;
import io.basestar.exception.HasExceptionMetadata;

public class MissingPropertyException extends RuntimeException implements HasExceptionMetadata {

    public static final int STATUS = 400;

    public static final String CODE = "MissingProperty";

    public static final String PROPERTY = "property";

    private final String property;

    public MissingPropertyException(final String property) {

        super("Property " + property + " not found");
        this.property = property;
    }

    @Override
    public ExceptionMetadata getMetadata() {

        return new ExceptionMetadata()
                .setStatus(STATUS)
                .setCode(CODE)
                .setMessage(getMessage())
                .putData(PROPERTY, property);
    }
}

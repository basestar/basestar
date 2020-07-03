package io.basestar.api.exception;

import io.basestar.exception.ExceptionMetadata;
import io.basestar.exception.HasExceptionMetadata;

public class InvalidQueryException extends RuntimeException implements HasExceptionMetadata {

    public static final int STATUS = 400;

    public static final String CODE = "InvalidQuery";

    private final String parameter;

    public InvalidQueryException(final String parameter, final String message) {

        super("Query parameter " + parameter + " invalid (" + message + ")");
        this.parameter = parameter;
    }

    @Override
    public ExceptionMetadata getMetadata() {

        return new ExceptionMetadata()
                .setStatus(STATUS)
                .setCode(CODE)
                .putData("parameter", parameter)
                .setMessage(getMessage());
    }
}

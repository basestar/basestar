package io.basestar.database.exception;

import io.basestar.exception.ExceptionMetadata;
import io.basestar.exception.HasExceptionMetadata;
import io.basestar.util.Name;

public class MissingArgumentException extends RuntimeException implements HasExceptionMetadata {

    public static final int STATUS = 400;

    public static final String CODE = "MissingArgument";

    public MissingArgumentException(final Name schema, final String argument) {

        super("Query schema " + schema + " requires argument " + argument + " but it was not provided");
    }

    @Override
    public ExceptionMetadata getMetadata() {

        return new ExceptionMetadata()
                .setStatus(STATUS)
                .setCode(CODE)
                .setMessage(getMessage());
    }
}

package io.basestar.storage.exception;

import io.basestar.exception.ExceptionMetadata;
import io.basestar.exception.HasExceptionMetadata;
import io.basestar.util.Name;

public class UnsupportedWriteException extends RuntimeException implements HasExceptionMetadata {

    public static final int STATUS = 405;

    public static final String CODE = "UnsupportedWrite";

    public UnsupportedWriteException(final Name schema) {

        this(schema, "unspecified");
    }

    public UnsupportedWriteException(final Name schema, final String reason) {

        super("Schema " + schema + "does not support write (reason: " + reason + ")");
    }

    @Override
    public ExceptionMetadata getMetadata() {

        return new ExceptionMetadata()
                .setStatus(STATUS)
                .setCode(CODE)
                .setMessage(getMessage());
    }
}

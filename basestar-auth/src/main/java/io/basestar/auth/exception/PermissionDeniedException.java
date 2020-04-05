package io.basestar.auth.exception;

import io.basestar.exception.ExceptionMetadata;
import io.basestar.exception.HasExceptionMetadata;

public class PermissionDeniedException extends RuntimeException implements HasExceptionMetadata {

    public static final int STATUS = 403;

    public static final String CODE = "PermissionDenied";

    public PermissionDeniedException(final String message) {

        super(message);
    }

    public PermissionDeniedException(final String message, final Exception cause) {

        super(message, cause);
    }

    @Override
    public ExceptionMetadata getMetadata() {

        return new ExceptionMetadata()
                .setStatus(STATUS)
                .setCode(CODE)
                .setMessage(getMessage());
    }
}

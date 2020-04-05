package io.basestar.auth.exception;

import io.basestar.exception.ExceptionMetadata;
import io.basestar.exception.HasExceptionMetadata;

public class AuthenticationFailedException extends RuntimeException implements HasExceptionMetadata {

    public static final int STATUS = 401;

    public static final String CODE = "AuthenticationFailed";

    public AuthenticationFailedException(final String message, final Throwable cause) {

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

package io.basestar.storage.exception;

import io.basestar.exception.ExceptionMetadata;
import io.basestar.exception.HasExceptionMetadata;

public class PagingTokenSyntaxException extends RuntimeException implements HasExceptionMetadata {

    public static final int STATUS = 400;

    public static final String CODE = "CorruptedToken";

    public PagingTokenSyntaxException(final String message) {
        super(message);
    }

    @Override
    public ExceptionMetadata getMetadata() {
        return new ExceptionMetadata()
                .setStatus(STATUS)
                .setCode(CODE)
                .setMessage(getMessage());
    }
}

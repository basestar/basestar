package io.basestar.api.exception;

import io.basestar.exception.ExceptionMetadata;
import io.basestar.exception.HasExceptionMetadata;

public class UnsupportedContentException extends RuntimeException implements HasExceptionMetadata {

    public static final int STATUS = 400;

    public static final String CODE = "UnsupportedContent";

    public UnsupportedContentException(final String contentType) {

        super(contentType == null ? "Content type must be specified" : ("Content of type \"" + contentType + "\" not supported"));
    }

    @Override
    public ExceptionMetadata getMetadata() {

        return new ExceptionMetadata()
                .setStatus(STATUS)
                .setCode(CODE)
                .setMessage(getMessage());
    }
}

package io.basestar.storage.exception;

import io.basestar.exception.ExceptionMetadata;
import io.basestar.exception.HasExceptionMetadata;

public class ObjectMissingException extends RuntimeException implements HasExceptionMetadata {

    public static final int STATUS = 404;

    public static final String CODE = "ObjectMissing";

    public ObjectMissingException(final String schema, final String id) {

        super(schema + " with id \"" + id  + "\" does not exist");
    }

    @Override
    public ExceptionMetadata getMetadata() {

        return new ExceptionMetadata()
                .setStatus(STATUS)
                .setCode(CODE)
                .setMessage(getMessage());
    }
}
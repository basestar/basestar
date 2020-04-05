package io.basestar.storage.exception;

import io.basestar.exception.ExceptionMetadata;
import io.basestar.exception.HasExceptionMetadata;

public class ObjectExistsException extends RuntimeException implements HasExceptionMetadata {

    public static final int STATUS = 409;

    public static final String CODE = "ObjectExists";

    public ObjectExistsException(final String schema, final String id) {

        super(schema + " with id \"" + id  + "\" already exists");
    }

    @Override
    public ExceptionMetadata getMetadata() {

        return new ExceptionMetadata()
                .setStatus(STATUS)
                .setCode(CODE)
                .setMessage(getMessage());
    }
}
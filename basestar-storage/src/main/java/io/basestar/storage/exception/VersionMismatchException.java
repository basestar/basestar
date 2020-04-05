package io.basestar.storage.exception;

import io.basestar.exception.ExceptionMetadata;
import io.basestar.exception.HasExceptionMetadata;

public class VersionMismatchException extends RuntimeException implements HasExceptionMetadata {

    public static final int STATUS = 409;

    public static final String CODE = "VersionMismatch";

    public VersionMismatchException(final String schema, final String id, final Long version) {

        super(schema + " with id \"" + id + "\" not at version " + version);
    }

    @Override
    public ExceptionMetadata getMetadata() {

        return new ExceptionMetadata()
                .setStatus(STATUS)
                .setCode(CODE)
                .setMessage(getMessage());
    }
}

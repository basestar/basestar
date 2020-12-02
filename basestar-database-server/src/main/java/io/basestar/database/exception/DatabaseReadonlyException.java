package io.basestar.database.exception;

import io.basestar.exception.ExceptionMetadata;
import io.basestar.exception.HasExceptionMetadata;

public class DatabaseReadonlyException extends RuntimeException implements HasExceptionMetadata {

    public static final int STATUS = 503;

    public static final String CODE = "DatabaseReadonly";

    public DatabaseReadonlyException() {

        super("Database is in readonly mode");
    }

    @Override
    public ExceptionMetadata getMetadata() {

        return new ExceptionMetadata()
                .setStatus(STATUS)
                .setCode(CODE)
                .setMessage(getMessage());
    }
}

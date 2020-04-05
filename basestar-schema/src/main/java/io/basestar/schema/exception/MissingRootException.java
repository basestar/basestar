package io.basestar.schema.exception;

import io.basestar.exception.ExceptionMetadata;
import io.basestar.exception.HasExceptionMetadata;

public class MissingRootException extends RuntimeException implements HasExceptionMetadata {

    public static final int STATUS = 400;

    public static final String CODE = "MissingRoot";

    public static final String TYPE = "type";

    private final String type;

    public MissingRootException(final String type) {

        super("Root " + type + " not found");
        this.type = type;
    }

    @Override
    public ExceptionMetadata getMetadata() {

        return new ExceptionMetadata()
                .setStatus(STATUS)
                .setCode(CODE)
                .setMessage(getMessage())
                .putData(TYPE, type);
    }
}

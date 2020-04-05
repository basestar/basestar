package io.basestar.storage.exception;

import io.basestar.exception.ExceptionMetadata;
import io.basestar.exception.HasExceptionMetadata;

public class UniqueIndexViolationException extends RuntimeException implements HasExceptionMetadata {

    public static final int STATUS = 409;

    public static final String CODE = "UniqueIndexViolation";

    public static final String INDEX = "index";

    private final String index;

    public UniqueIndexViolationException(final String schema, final String id, final String index) {

        super(schema + " with id \"" + id + "\" cannot be created because index values for " + index + " already exist");
        this.index = index;
    }

    @Override
    public ExceptionMetadata getMetadata() {

        return new ExceptionMetadata()
                .setStatus(STATUS)
                .setCode(CODE)
                .setMessage(getMessage())
                .putData(INDEX, index);
    }
}

package io.basestar.storage.exception;

import io.basestar.exception.ExceptionMetadata;
import io.basestar.exception.HasExceptionMetadata;

public class CorruptedIndexException extends RuntimeException implements HasExceptionMetadata {

    public static final int STATUS = 409;

    public static final String CODE = "CorruptedIndex";

    public static final String INDEX = "index";

    private final String index;

    public CorruptedIndexException(final String type, final String index) {

        super(type + "." + index + " is corrupted");
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

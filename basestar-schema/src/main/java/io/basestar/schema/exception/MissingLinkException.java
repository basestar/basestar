package io.basestar.schema.exception;

import io.basestar.exception.ExceptionMetadata;
import io.basestar.exception.HasExceptionMetadata;

public class MissingLinkException extends RuntimeException implements HasExceptionMetadata {

    public static final int STATUS = 400;

    public static final String CODE = "MissingLink";

    public static final String LINK = "link";

    private final String link;

    public MissingLinkException(final String link) {

        super("Property " + link + " not found");
        this.link = link;
    }

    @Override
    public ExceptionMetadata getMetadata() {

        return new ExceptionMetadata()
                .setStatus(STATUS)
                .setCode(CODE)
                .setMessage(getMessage())
                .putData(LINK, link);
    }
}

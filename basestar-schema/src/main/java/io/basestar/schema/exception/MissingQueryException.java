package io.basestar.schema.exception;

import com.google.common.base.Joiner;
import io.basestar.exception.ExceptionMetadata;
import io.basestar.exception.HasExceptionMetadata;

import java.util.Collection;

public class MissingQueryException extends RuntimeException implements HasExceptionMetadata {

    public static final int STATUS = 400;

    public static final String CODE = "MissingQuery";

    public static final String QUERY = "query";

    private final String query;

    public MissingQueryException(final String query, final Collection<String> names) {

        super("Query " + query + " not found" + (names.isEmpty() ? "" : ", did you mean: " + Joiner.on(", ").join(names)));
        this.query = query;
    }

    @Override
    public ExceptionMetadata getMetadata() {

        return new ExceptionMetadata()
                .setStatus(STATUS)
                .setCode(CODE)
                .setMessage(getMessage())
                .putData(QUERY, query);
    }
}
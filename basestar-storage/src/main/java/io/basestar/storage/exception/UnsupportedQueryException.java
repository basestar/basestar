package io.basestar.storage.exception;

import io.basestar.expression.Expression;

public class UnsupportedQueryException extends RuntimeException {

    public UnsupportedQueryException(final String schema, final Expression expression) {

        super("Schema " + schema + " does not support query " + expression);
    }
}

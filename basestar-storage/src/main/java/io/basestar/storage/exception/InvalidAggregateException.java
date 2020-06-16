package io.basestar.storage.exception;

public class InvalidAggregateException extends RuntimeException {

    public InvalidAggregateException(final String aggregate) {

        super("Invalid arguments for aggregate: " + aggregate);
    }
}

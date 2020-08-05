package io.basestar.exception;

public class InvalidDateException extends RuntimeException {

    public InvalidDateException(final Object value) {

        super("Cannot extract date from " + value);
    }
}

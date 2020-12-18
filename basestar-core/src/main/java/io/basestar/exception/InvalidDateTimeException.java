package io.basestar.exception;

public class InvalidDateTimeException extends RuntimeException {

    public InvalidDateTimeException(final Object value) {

        super("Cannot extract date/time from " + value);
    }
}

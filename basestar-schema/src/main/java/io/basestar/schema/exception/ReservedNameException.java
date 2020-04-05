package io.basestar.schema.exception;

public class ReservedNameException extends RuntimeException {

    public ReservedNameException(final String name) {

        super("The name " + name + " is reserved");
    }
}

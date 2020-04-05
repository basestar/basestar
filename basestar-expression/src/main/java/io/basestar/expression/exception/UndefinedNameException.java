package io.basestar.expression.exception;

public class UndefinedNameException extends RuntimeException {

    public UndefinedNameException(final String name) {

        super("Name " + name + " not found");
    }


}

package io.basestar.mapper.exception;

public class MappingException extends RuntimeException {

    public MappingException(final String name, final Exception e) {

        super("Failed to map " + name, e);
    }
}

package io.basestar.expression.exception;

import java.util.Arrays;
import java.util.stream.Collectors;

public class MethodNotFoundException extends RuntimeException {

    private final Class<?> type;

    private final String method;

    private final Class<?>[] args;

    public MethodNotFoundException(final Class<?> type, final String method, final Class<?>[] args) {

        super("Type " + type.getName() + " has no method " + method
                + " with arguments " + Arrays.stream(args).map(Class::getName)
                .collect(Collectors.joining(", ")));

        this.type = type;
        this.method = method;
        this.args = args;
    }
}

package io.basestar.expression.exception;

public class MemberNotFoundException extends RuntimeException {

    private final Class<?> type;

    private final String member;

    public MemberNotFoundException(final Class<?> type, final String member) {

        super("Type " + type.getName() + " has no member " + member);

        this.type = type;
        this.member = member;
    }
}

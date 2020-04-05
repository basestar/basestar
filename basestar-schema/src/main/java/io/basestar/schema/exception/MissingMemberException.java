package io.basestar.schema.exception;

import io.basestar.exception.ExceptionMetadata;
import io.basestar.exception.HasExceptionMetadata;

public class MissingMemberException extends RuntimeException implements HasExceptionMetadata {

    public static final int STATUS = 400;

    public static final String CODE = "MissingMember";

    public static final String MEMBER = "member";

    private final String member;

    public MissingMemberException(final String member) {

        super("Member " + member + " not found");
        this.member = member;
    }

    @Override
    public ExceptionMetadata getMetadata() {

        return new ExceptionMetadata()
                .setStatus(STATUS)
                .setCode(CODE)
                .setMessage(getMessage())
                .putData(MEMBER, member);
    }
}

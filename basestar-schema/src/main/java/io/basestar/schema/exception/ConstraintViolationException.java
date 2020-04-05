package io.basestar.schema.exception;

import com.google.common.collect.ImmutableSet;
import io.basestar.exception.ExceptionMetadata;
import io.basestar.exception.HasExceptionMetadata;
import io.basestar.schema.Constraint;

import java.util.Set;

public class ConstraintViolationException extends RuntimeException implements HasExceptionMetadata {

    public static final int STATUS = 400;

    public static final String CODE = "ConstraintViolation";

    public static final String VIOLATIONS = "violations";

    private final Set<Constraint.Violation> violations;

    public ConstraintViolationException(final Set<Constraint.Violation> violations) {

        super("Property constraints violated " + violations.toString());
        this.violations = ImmutableSet.copyOf(violations);
    }

    @Override
    public ExceptionMetadata getMetadata() {

        return new ExceptionMetadata()
                .setStatus(STATUS)
                .setCode(CODE)
                .setMessage("Property constraints violated")
                .putData(VIOLATIONS, violations);
    }
}

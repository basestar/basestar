package io.basestar.schema.jsr380;

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;

public class WhenConstraintValidator implements ConstraintValidator<Assert, Object> {

    @Override
    public void initialize(final Assert constraintAnnotation) {

    }

    @Override
    public boolean isValid(final Object o, final ConstraintValidatorContext context) {

        return false;
    }
}

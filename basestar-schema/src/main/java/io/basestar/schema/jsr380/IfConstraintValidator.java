package io.basestar.schema.jsr380;

import com.google.common.collect.ImmutableMap;
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.schema.Reserved;

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;

public class IfConstraintValidator implements ConstraintValidator<If, Object> {

    private Expression expression;

    public void initialize(final If annotation) {

        this.expression = Expression.parse(annotation.value()).bind(Context.init());
    }

    @Override
    public boolean isValid(final Object value, final ConstraintValidatorContext context) {

        if(value == null) {
            return true;
        }
        return expression.evaluatePredicate(Context.init(ImmutableMap.of(Reserved.VALUE, value)));
    }
}

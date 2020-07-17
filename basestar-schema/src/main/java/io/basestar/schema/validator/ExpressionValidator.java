package io.basestar.schema.validator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import lombok.Data;

@Data
public class ExpressionValidator implements Validator {

    public static final String TYPE = "expression";

    private final Expression expression;

    @JsonCreator
    public static ExpressionValidator from(final Expression expression) {

        return new ExpressionValidator(expression);
    }

    @JsonValue
    public Expression getExpression() {

        return expression;
    }

    @Override
    public String type() {

        return TYPE;
    }

    @Override
    public String defaultMessage() {

        return null;
    }

    @Override
    public boolean validate(final Context context, final Object value) {

        return !expression.evaluatePredicate(context);
    }
}

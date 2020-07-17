package io.basestar.schema.validator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.collect.ImmutableList;
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.schema.jsr380.If;
import io.basestar.schema.use.Use;
import lombok.Data;

import javax.validation.Payload;
import java.lang.annotation.Annotation;
import java.util.List;

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
    public boolean validate(final Use<?> type, final Context context, final Object value) {

        return !expression.evaluatePredicate(context);
    }

    @Override
    public List<? extends Annotation> jsr380(final Use<?> type, final String message, final Class<?>[] groups, final Class<? extends Payload>[] payload) {

        return ImmutableList.of(new If() {

            @Override
            public Class<? extends Annotation> annotationType() {

                return If.class;
            }

            @Override
            public String value() {

                return expression.toString();
            }

            @Override
            public String message() {

                return message;
            }

            @Override
            public Class<?>[] groups() {

                return groups;
            }

            @Override
            public Class<? extends Payload>[] payload() {

                return payload;
            }
        });
    }
}

package io.basestar.schema.validation;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.expression.constant.NameConstant;
import io.basestar.expression.logical.Not;
import io.basestar.schema.Reserved;
import io.basestar.schema.jsr380.Assert;
import io.basestar.schema.use.Use;
import io.basestar.type.AnnotationContext;
import io.basestar.util.Nullsafe;
import lombok.Data;

import javax.validation.constraints.AssertFalse;
import javax.validation.constraints.AssertTrue;
import java.lang.annotation.Annotation;
import java.util.Map;
import java.util.Optional;

@SuppressWarnings("unused")
public class AssertValidation implements Validation {

    public static final String TYPE = "assert";

    @Override
    public String type() {

        return TYPE;
    }

    @Override
    public Class<? extends Validator> validatorClass() {

        return Validator.class;
    }

    @Override
    public Optional<Validation.Validator> fromJsr380(final Use<?> type, final Annotation annotation) {

        final Class<? extends Annotation> annotationType = annotation.annotationType();
        if (Assert.class.equals(annotationType)) {
            return Optional.of(fromJsr380((Assert) annotation));
        } else if (AssertTrue.class.equals(annotationType)) {
            return Optional.of(fromJsr380((AssertTrue) annotation));
        } else if (AssertFalse.class.equals(annotationType)) {
            return Optional.of(fromJsr380((AssertFalse) annotation));
        } else {
            return Optional.empty();
        }
    }

    public static Validator fromJsr380(final Assert annotation) {

        return new Validator(Expression.parse(annotation.value()));
    }

    public static Validator fromJsr380(final AssertTrue annotation) {

        return new Validator(new NameConstant(Reserved.VALUE_NAME));
    }

    public static Validator fromJsr380(final AssertFalse annotation) {

        return new Validator(new Not(new NameConstant(Reserved.VALUE_NAME)));
    }

    @Data
    public static class Validator implements Validation.Validator {

        private final Expression expression;

        @JsonCreator
        public Validator(final String expression) {

            this(Expression.parse(expression));
        }

        @JsonCreator
        public Validator(@JsonProperty("expression") final Expression expression) {

            this.expression = Nullsafe.require(expression).bind(Context.init());
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
        public Assert toJsr380(final Use<?> type, final Map<String, Object> values) {

            return new AnnotationContext<>(Assert.class, ImmutableMap.<String, Object>builder().putAll(values)
                    .put("value", expression.toString())
                    .build()).annotation();
        }

        @Override
        public Object shorthand() {

            return expression;
        }
    }
}
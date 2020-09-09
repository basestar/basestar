package io.basestar.schema.validation;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import io.basestar.expression.Context;
import io.basestar.schema.use.Use;
import io.basestar.type.AnnotationContext;
import io.basestar.util.Nullsafe;
import lombok.Data;

import javax.validation.constraints.DecimalMin;
import javax.validation.constraints.Min;
import java.lang.annotation.Annotation;
import java.math.BigDecimal;
import java.util.Map;
import java.util.Optional;

public class MinValidation implements Validation {

    public static final String TYPE = "min";

    @Override
    public String type() {

        return TYPE;
    }

    @Override
    public Class<? extends Validation.Validator> validatorClass() {

        return Validator.class;
    }

    @Override
    public Optional<Validation.Validator> fromJsr380(final Use<?> type, final Annotation annotation) {

        final Class<? extends Annotation> annotationType = annotation.annotationType();
        if(Min.class.isAssignableFrom(annotationType)) {
            return Optional.of(fromJsr380((Min)annotation));
        } else if(DecimalMin.class.isAssignableFrom(annotationType)) {
            return Optional.of(fromJsr380((DecimalMin)annotation));
        } else {
            return Optional.empty();
        }
    }

    public static Validator fromJsr380(final DecimalMin annotation) {

        return new Validator(new BigDecimal(annotation.value()), !annotation.inclusive());
    }

    public static Validator fromJsr380(final Min annotation) {

        return new Validator(new BigDecimal(annotation.value()), false);
    }

    @Data
    public static class Validator implements Validation.Validator {

        private final BigDecimal value;

        // Inversion of JSR380 setting because we prefer booleans to be default false
        @JsonInclude(JsonInclude.Include.NON_DEFAULT)
        private final boolean exclusive;

        @JsonCreator
        public Validator(final Long value) {

            this(BigDecimal.valueOf(value), false);
        }

        @JsonCreator
        public Validator(final Double value) {

            this(BigDecimal.valueOf(value), false);
        }

        @JsonCreator
        public Validator(final String value) {

            this(new BigDecimal(value), false);
        }

        @JsonCreator
        public Validator(@JsonProperty("value") final BigDecimal value, @JsonProperty("exclusive") final Boolean exclusive) {

            this.value = Nullsafe.require(value);
            this.exclusive = Nullsafe.orDefault(exclusive);
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

            return true;
        }

        @Override
        public DecimalMin toJsr380(final Use<?> type, final Map<String, Object> values) {

            return new AnnotationContext<>(DecimalMin.class, ImmutableMap.<String, Object>builder().putAll(values)
                    .put("value", value.toString())
                    .put("inclusive", !exclusive)
                    .build()).annotation();
        }

        @Override
        public Object shorthand() {

            return exclusive ? this : value;
        }
    }
}


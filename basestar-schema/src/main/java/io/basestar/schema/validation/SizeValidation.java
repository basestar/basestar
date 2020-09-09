package io.basestar.schema.validation;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import io.basestar.expression.Context;
import io.basestar.schema.use.Use;
import io.basestar.type.AnnotationContext;
import io.basestar.util.Nullsafe;
import lombok.Data;

import javax.validation.constraints.Size;
import java.lang.annotation.Annotation;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class SizeValidation implements Validation {

    public static final String TYPE = "size";

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
        if(Size.class.isAssignableFrom(annotationType)) {
            return Optional.of(fromJsr380((Size)annotation));
        } else {
            return Optional.empty();
        }
    }

    public static Validator fromJsr380(final Size annotation) {

        final Integer min = annotation.min() == 0 ? null : annotation.min();
        final Integer max = annotation.max() == Integer.MAX_VALUE ? null : annotation.max();
        return new Validator(min, max);
    }

    @Data
    public static class Validator implements Validation.Validator {

        private final Integer min;

        private final Integer max;

        @JsonCreator
        public Validator(final Integer exact) {

            this(exact, exact);
        }

        @JsonCreator
        public Validator(@JsonProperty("min") final Integer min, @JsonProperty("max") final Integer max) {

            this.min = min;
            this.max = max;
            if (this.min == null && this.max == null) {
                throw new IllegalStateException("At least one of min, max must be provided");
            }
        }

        @Override
        public String type() {

            return TYPE;
        }

        @Override
        public String defaultMessage() {

            if (min != null && max != null) {
                if (min.equals(max)) {
                    return "size must be " + min;
                } else {
                    return "size must be between " + min + " and " + max;
                }
            } else if (min != null) {
                return "size must be at least " + min;
            } else if (max != null) {
                return "size must be at most " + max;
            } else {
                return null;
            }
        }

        @Override
        public boolean validate(final Use<?> type, final Context context, final Object value) {

            return true;
        }

        @Override
        public Size toJsr380(final Use<?> type, final Map<String, Object> values) {

            return new AnnotationContext<>(Size.class, ImmutableMap.<String, Object>builder().putAll(values)
                    .put("min", Nullsafe.orDefault(min, 0))
                    .put("max", Nullsafe.orDefault(max, Integer.MAX_VALUE))
                    .build()).annotation();
        }

        @Override
        public Object shorthand() {

            if(Objects.equals(min, max)) {
                return min;
            } else {
                return this;
            }
        }
    }
}

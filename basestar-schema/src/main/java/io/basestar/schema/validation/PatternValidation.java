package io.basestar.schema.validation;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.basestar.expression.Context;
import io.basestar.schema.use.Use;
import io.basestar.type.AnnotationContext;
import io.basestar.util.Nullsafe;
import lombok.Data;

import java.lang.annotation.Annotation;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class PatternValidation implements Validation {

    public static final String TYPE = "pattern";

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
        if(javax.validation.constraints.Pattern.class.isAssignableFrom(annotationType)) {
            return Optional.of(fromJsr380((javax.validation.constraints.Pattern)annotation));
        } else {
            return Optional.empty();
        }
    }

    public static Validator fromJsr380(final javax.validation.constraints.Pattern annotation) {

        return new Validator(annotation.regexp(), ImmutableSet.copyOf(annotation.flags()));
    }

    private static int flags(final Set<javax.validation.constraints.Pattern.Flag> flags) {

        return flags.stream().mapToInt(javax.validation.constraints.Pattern.Flag::getValue)
                .reduce(0, (a, b) -> a | b);
    }

    private static Set<javax.validation.constraints.Pattern.Flag> flags(final int flags) {

        return Arrays.stream(javax.validation.constraints.Pattern.Flag.values())
                .filter(v -> (v.getValue() & flags) == v.getValue())
                .collect(Collectors.toSet());
    }

    @Data
    public static class Validator implements Validation.Validator {

        private final Pattern regex;

        @JsonCreator
        public Validator(final String regex) {

            this(regex, null);
        }

        @JsonCreator
        @SuppressWarnings("MagicConstant")
        public Validator(@JsonProperty("regex") final String regex, @JsonProperty("flags") final Set<javax.validation.constraints.Pattern.Flag> flags) {

            this.regex = Pattern.compile(Nullsafe.require(regex), flags(Nullsafe.orDefault(flags)));
        }

        public String getRegex() {

            return regex.toString();
        }

        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        public Set<javax.validation.constraints.Pattern.Flag> getFlags() {

            return flags(regex.flags());
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

            if(value instanceof String) {
                return regex.matcher((String)value).matches();
            } else {
                return false;
            }
        }

        @Override
        public javax.validation.constraints.Pattern toJsr380(final Use<?> type, final Map<String, Object> values) {

            return new AnnotationContext<>(javax.validation.constraints.Pattern.class, ImmutableMap.<String, Object>builder().putAll(values)
                    .put("regexp", getRegex())
                    .put("flags", getFlags().toArray(new javax.validation.constraints.Pattern.Flag[0]))
                    .build()).annotation();
        }

        @Override
        public Object shorthand() {

            return regex.flags() == 0 ? getRegex() : this;
        }

        // Pattern does not implement equals/hash code properly

        @Override
        public boolean equals(final Object o) {

            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final Validator validator = (Validator) o;
            return Objects.equals(regex.toString(), validator.regex.toString())
                    && Objects.equals(regex.flags(), validator.regex.flags());
        }

        @Override
        public int hashCode() {

            return Objects.hash(regex.toString(), regex.flags());
        }
    }
}

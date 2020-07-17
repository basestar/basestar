package io.basestar.schema.validator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.google.common.collect.ImmutableList;
import io.basestar.expression.Context;
import io.basestar.schema.use.Use;
import lombok.Builder;
import lombok.Data;

import javax.validation.Payload;
import java.lang.annotation.Annotation;
import java.util.List;
import java.util.regex.Pattern;

@Data
@Builder(builderClassName = "Builder", setterPrefix = "set")
@JsonDeserialize(builder = RegexValidator.Builder.class)
public class RegexValidator implements Validator {

    public static final String TYPE = "regex";

    private final Pattern pattern;

    @JsonPOJOBuilder(withPrefix = "set")
    public static class Builder {

        public Builder setPattern(final String pattern) {

            this.pattern = Pattern.compile(pattern);
            return this;
        }

        @JsonCreator
        public static Builder from(final String pattern) {

            return new Builder().setPattern(pattern);
        }
    }

    @JsonValue
    public String getPattern() {

        return pattern.toString();
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
    public List<? extends Annotation> jsr380(final Use<?> type,  final String message, final Class<?>[] groups, final Class<? extends Payload>[] payload) {

        return ImmutableList.of(new javax.validation.constraints.Pattern() {

            @Override
            public Class<? extends Annotation> annotationType() {

                return javax.validation.constraints.Pattern.class;
            }

            @Override
            public String regexp() {

                return pattern.toString();
            }

            @Override
            public Flag[] flags() {

                return new Flag[0];
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

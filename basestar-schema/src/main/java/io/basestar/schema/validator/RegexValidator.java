package io.basestar.schema.validator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import io.basestar.expression.Context;
import lombok.Builder;
import lombok.Data;

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
    public boolean validate(final Context context, final Object value) {

        return true;
    }
}

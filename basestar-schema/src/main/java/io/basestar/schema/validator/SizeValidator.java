package io.basestar.schema.validator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import io.basestar.expression.Context;
import lombok.Builder;
import lombok.Data;

@Data
@Builder(builderClassName = "Builder", setterPrefix = "set")
@JsonDeserialize(builder = SizeValidator.Builder.class)
public class SizeValidator implements Validator {

    public static final String TYPE = "size";

    private final Long min;

    private final Long max;

    @JsonPOJOBuilder(withPrefix = "set")
    public static class Builder {

        @JsonCreator
        public static Builder from(final Long exact) {

            return new Builder().setMin(exact).setMax(exact);
        }
    }

    @Override
    public String type() {

        return TYPE;
    }

    @Override
    public String defaultMessage() {

        if(min != null && max != null) {
            if(min.equals(max)) {
                return "size must be " + min;
            } else {
                return "size must be between " + min + " and " + max;
            }
        } else if(min != null) {
            return "size must be at least " + min;
        } else if(max != null) {
            return "size must be at most " + max;
        } else {
            return null;
        }
    }

    @Override
    public boolean validate(final Context context, final Object value) {

        return true;
    }
}

package io.basestar.schema.validator;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import io.basestar.expression.Context;
import lombok.Builder;
import lombok.Data;

@Data
@Builder(builderClassName = "Builder", setterPrefix = "set")
@JsonDeserialize(builder = RangeValidator.Builder.class)
public class RangeValidator implements Validator {

    public static final String TYPE = "range";

    private final Object gt;

    private final Object gte;

    private final Object lt;

    private final Object lte;

    @JsonPOJOBuilder(withPrefix = "set")
    public static class Builder {

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

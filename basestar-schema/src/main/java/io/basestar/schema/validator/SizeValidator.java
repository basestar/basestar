package io.basestar.schema.validator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.google.common.collect.ImmutableList;
import io.basestar.expression.Context;
import io.basestar.schema.use.Use;
import lombok.Builder;
import lombok.Data;

import javax.validation.Payload;
import javax.validation.constraints.Size;
import java.lang.annotation.Annotation;
import java.util.List;

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
    public boolean validate(final Use<?> type, final Context context, final Object value) {

        return true;
    }

    @Override
    public List<? extends Annotation> jsr380(final Use<?> type, final String message, final Class<?>[] groups, final Class<? extends Payload>[] payload) {

        return ImmutableList.of(new Size() {

            @Override
            public Class<? extends Annotation> annotationType() {

                return Size.class;
            }

            @Override
            public int min() {

                return min == null ? 0 : min.intValue();
            }

            @Override
            public int max() {

                return max == null ? Integer.MAX_VALUE : max.intValue();
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

package io.basestar.schema.validator;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import io.basestar.expression.Context;
import io.basestar.schema.use.Use;
import lombok.Builder;
import lombok.Data;

import javax.validation.Payload;
import javax.validation.constraints.DecimalMax;
import javax.validation.constraints.DecimalMin;
import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.List;

@Data
@Builder(builderClassName = "Builder", setterPrefix = "set")
@JsonDeserialize(builder = RangeValidator.Builder.class)
public class RangeValidator implements Validator {

    public static final String TYPE = "range";

    private final Object lt;

    private final Object gt;

    private final Object lte;

    private final Object gte;

    RangeValidator(final Object lt, final Object gt, final Object lte, final Object gte) {

        if(lt != null && lte != null) {
            throw new IllegalStateException("Cannot specify lt and lte");
        }
        if(gt != null && gte != null) {
            throw new IllegalStateException("Cannot specify gt and gte");
        }
        this.lt = lt;
        this.gt = gt;
        this.lte = lte;
        this.gte = gte;
    }

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
    public boolean validate(final Use<?> type, final Context context, final Object value) {

        return true;
    }

    @Override
    public List<? extends Annotation> jsr380(final Use<?> type, final String message, final Class<?>[] groups, final Class<? extends Payload>[] payload) {

        final List<Annotation> annotations = new ArrayList<>();
        if(lt != null) {
            annotations.add(min(type, message, groups, payload, lt, false));
        } else if(lte != null) {
            annotations.add(min(type, message, groups, payload, lte, true));
        }
        if(gt != null) {
            annotations.add(max(type, message, groups, payload, gt, false));
        } else if(gte != null) {
            annotations.add(max(type, message, groups, payload, gte, true));
        }
        return annotations;
    }

    private Annotation min(final Use<?> type, final String message, final Class<?>[] groups, final Class<? extends Payload>[] payload, final Object value, final boolean inclusive) {

        return new DecimalMin() {

            @Override
            public Class<? extends Annotation> annotationType() {

                return DecimalMin.class;
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

            @Override
            public String value() {

                return value.toString();
            }

            @Override
            public boolean inclusive() {

                return inclusive;
            }
        };
    }

    private Annotation max(final Use<?> type, final String message, final Class<?>[] groups, final Class<? extends Payload>[] payload, final Object value, final boolean inclusive) {

        return new DecimalMax() {

            @Override
            public Class<? extends Annotation> annotationType() {

                return DecimalMax.class;
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

            @Override
            public String value() {

                return value.toString();
            }

            @Override
            public boolean inclusive() {

                return inclusive;
            }
        };
    }
}

package io.basestar.schema.validator;

import io.basestar.expression.Context;
import io.basestar.schema.use.Use;

import javax.validation.Payload;
import java.lang.annotation.Annotation;
import java.util.List;

public interface Validator {

    String type();

    String defaultMessage();

    boolean validate(Use<?> type, Context context, Object value);

    default List<? extends Annotation> jsr380(Use<?> type) {

        return jsr380(type, defaultMessage());
    }

    @SuppressWarnings("unchecked")
    default List<? extends Annotation> jsr380(final Use<?> type, final String message) {

        return jsr380(type, message, new Class<?>[0], new Class[0]);
    }

    List<? extends Annotation> jsr380(Use<?> type, String message, Class<?>[] groups, Class<? extends Payload>[] payload);
}

package io.basestar.schema.validator;

import io.basestar.expression.Context;

public interface Validator {

    String type();

    String defaultMessage();

    boolean validate(Context context, Object value);
}

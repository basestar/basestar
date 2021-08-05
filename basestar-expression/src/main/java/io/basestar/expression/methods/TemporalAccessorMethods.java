package io.basestar.expression.methods;

import java.io.Serializable;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;

@SuppressWarnings("unused")
public abstract class TemporalAccessorMethods<T extends TemporalAccessor> implements Serializable {

    public String tostring(final T target, final String format) {

        return DateTimeFormatter.ofPattern(format).format(target);
    }
}

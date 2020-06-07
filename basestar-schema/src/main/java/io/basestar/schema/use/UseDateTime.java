package io.basestar.schema.use;

import io.basestar.schema.exception.InvalidTypeException;
import io.swagger.v3.oas.models.media.DateTimeSchema;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.Date;

public class UseDateTime implements UseScalar<LocalDateTime> {

    public static final UseDateTime DEFAULT = new UseDateTime();

    public static final String NAME = "datetime";

    @Override
    public <R> R visit(final Visitor<R> visitor) {

        return visitor.visitDateTime(this);
    }

    public static UseDateTime from(final Object config) {

        return DEFAULT;
    }

    @Override
    public LocalDateTime create(final Object value, final boolean expand, final boolean suppress) {

        if(value == null) {
            return null;
        } else if(value instanceof String) {
            return LocalDateTime.parse((String)value, DateTimeFormatter.ISO_LOCAL_DATE_TIME);
        } else if(value instanceof TemporalAccessor) {
            return LocalDateTime.from((TemporalAccessor)value);
        } else if(value instanceof Date) {
            return LocalDateTime.from(((Date) value).toInstant());
        } else if(suppress) {
            return null;
        } else {
            throw new InvalidTypeException();
        }
    }

    @Override
    public Code code() {

        return Code.DATETIME;
    }

    @Override
    public Object toJson() {

        return NAME;
    }

    @Override
    public void serializeValue(final LocalDateTime value, final DataOutput out) throws IOException {

        UseString.DEFAULT.serializeValue(value.toString(), out);
    }

    @Override
    public LocalDateTime deserializeValue(final DataInput in) throws IOException {

        return LocalDateTime.parse(UseString.DEFAULT.deserializeValue(in));
    }

    @Override
    public io.swagger.v3.oas.models.media.Schema<?> openApi() {

        return new DateTimeSchema();
    }
}
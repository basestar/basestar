package io.basestar.schema.use;

import io.basestar.schema.exception.InvalidTypeException;
import io.swagger.v3.oas.models.media.DateSchema;
import io.swagger.v3.oas.models.media.Schema;
import lombok.Data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.Date;

@Data
public class UseDate implements UseScalar<LocalDate> {

    public static final UseDate DEFAULT = new UseDate();

    public static final String NAME = "date";

    @Override
    public <R> R visit(final Visitor<R> visitor) {

        return visitor.visitDate(this);
    }

    public static UseDate from(final Object config) {

        return DEFAULT;
    }

    @Override
    public LocalDate create(final Object value, final boolean expand, final boolean suppress) {

        if(value == null) {
            return null;
        } else if(value instanceof String) {
            return LocalDate.parse((String)value, DateTimeFormatter.ISO_LOCAL_DATE);
        } else if(value instanceof TemporalAccessor) {
            return LocalDate.from((TemporalAccessor) value);
        } else if(value instanceof Date) {
            return LocalDate.from(((Date) value).toInstant());
        } else if(suppress) {
            return null;
        } else {
            throw new InvalidTypeException();
        }
    }

    @Override
    public Code code() {

        return Code.DATE;
    }

    @Override
    public Object toJson() {

        return NAME;
    }

    @Override
    public void serializeValue(final LocalDate value, final DataOutput out) throws IOException {

        UseString.DEFAULT.serializeValue(value.toString(), out);
    }

    @Override
    public LocalDate deserializeValue(final DataInput in) throws IOException {

        return LocalDate.parse(UseString.DEFAULT.deserializeValue(in));
    }

    @Override
    public Schema<?> openApi() {

        return new DateSchema();
    }
}

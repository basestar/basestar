package io.basestar.schema.use;

import io.basestar.schema.exception.InvalidTypeException;
import lombok.Data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Integer Type
 *
 * Stored as 64bit (long) integer.
 *
 * <strong>Example</strong>
 * <pre>
 * type: integer
 * </pre>
 */

@Data
public class UseInteger implements UseScalar<Long> {

    public static UseInteger DEFAULT = new UseInteger();

    public static final String NAME = "integer";

    @Override
    public <R> R visit(final Visitor<R> visitor) {

        return visitor.visitInteger(this);
    }

    public static UseInteger from(final Object config) {

        return new UseInteger();
    }

    @Override
    public Object toJson() {

        return NAME;
    }

    @Override
    public Long create(final Object value) {

        if(value == null) {
            return null;
        } else if(value instanceof Boolean) {
            return ((Boolean)value) ? 1L : 0L;
        } else if(value instanceof Number) {
            return ((Number)value).longValue();
        } else if(value instanceof String) {
            return Long.parseLong((String)value);
        } else {
            throw new InvalidTypeException();
        }
    }

    @Override
    public Code code() {

        return Code.INTEGER;
    }

    @Override
    public void serializeValue(final Long value, final DataOutput out) throws IOException {

        out.writeLong(value);
    }

    @Override
    public Long deserializeValue(final DataInput in) throws IOException {

        return in.readLong();
    }

    @Override
    public String toString() {

        return NAME;
    }

//    @Override
//    public Map<String, Object> openApiType() {
//
//        return ImmutableMap.of(
//                "type", "number"
//        );
//    }
}

package io.basestar.schema.use;

import io.basestar.schema.exception.InvalidTypeException;
import lombok.Data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Number Type
 *
 * Stored as double-precision floating point.
 *
 * <strong>Example</strong>
 * <pre>
 * type: number
 * </pre>
 */

@Data
public class UseNumber implements UseScalar<Double> {

    public static UseNumber DEFAULT = new UseNumber();

    public static final String NAME = "number";

    public static UseNumber from(final Object config) {

        return new UseNumber();
    }

    @Override
    public <R> R visit(final Visitor<R> visitor) {

        return visitor.visitNumber(this);
    }

    @Override
    public Object toJson() {

        return NAME;
    }

    @Override
    public Double create(final Object value) {

        if(value == null) {
            return null;
        } else if(value instanceof Boolean) {
            return ((Boolean)value) ? 1.0 : 0.0;
        } else if(value instanceof Number) {
            return ((Number)value).doubleValue();
        } else if(value instanceof String) {
            return Double.parseDouble((String)value);
        } else {
            throw new InvalidTypeException();
        }
    }

    @Override
    public Code code() {

        return Code.NUMBER;
    }

    @Override
    public void serializeValue(final Double value, final DataOutput out) throws IOException {

        out.writeDouble(value);
    }

    @Override
    public Double deserializeValue(final DataInput in) throws IOException {

        return in.readDouble();
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

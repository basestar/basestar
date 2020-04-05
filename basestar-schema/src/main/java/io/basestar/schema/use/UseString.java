package io.basestar.schema.use;

import com.google.common.base.Charsets;
import io.basestar.schema.exception.InvalidTypeException;
import lombok.Data;
import lombok.RequiredArgsConstructor;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

/**
 * String Type
 *
 * <strong>Example</strong>
 * <pre>
 * type: string
 * </pre>
 */

@Data
@RequiredArgsConstructor
public class UseString implements UseScalar<String> {

    public static UseString DEFAULT = new UseString(null);

    public static final String NAME = "string";

    private final String pattern;

    public UseString() {

        this(null);
    }

    public static UseString from(final Object config) {

        if(config == null) {
            return DEFAULT;
        } else if(config instanceof String) {
            return new UseString((String)config);
        } else if(config instanceof Map) {
            return new UseString((String)((Map)config).get("pattern"));
        } else {
            throw new InvalidTypeException();
        }
    }

    @Override
    public <R> R visit(final Visitor<R> visitor) {

        return visitor.visitString(this);
    }

    @Override
    public Object toJson() {

        return NAME;
    }

    @Override
    public String create(final Object value) {

        if(value == null) {
            return null;
        } else if(value instanceof Boolean || value instanceof Number) {
            return value.toString();
        } else if(value instanceof String) {
            return (String)value;
        } else {
            throw new InvalidTypeException();
        }
    }

    @Override
    public Code code() {

        return Code.STRING;
    }

    @Override
    public void serializeValue(final String value, final DataOutput out) throws IOException {

        final byte[] buffer = value.getBytes(Charsets.UTF_8);
        out.writeInt(buffer.length);
        out.write(buffer);
    }

    @Override
    public String deserializeValue(final DataInput in) throws IOException {

        final int length = in.readInt();
        final byte[] buffer = new byte[length];
        in.readFully(buffer);
        return new String(buffer, Charsets.UTF_8);
    }

    @Override
    public String toString() {

        return NAME;
    }

//    @Override
//    public Map<String, Object> openApiType() {
//
//        return ImmutableMap.of(
//                "type", "string"
//        );
//    }
}

package io.basestar.schema.use;

import io.basestar.expression.type.exception.TypeConversionException;
import io.basestar.schema.secret.Secret;
import io.basestar.util.Name;
import io.swagger.v3.oas.models.media.Schema;
import io.swagger.v3.oas.models.media.StringSchema;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Set;

@Data
@Slf4j
public class UseSecret implements UseStringLike<Secret> {

    public static final UseSecret DEFAULT = new UseSecret();

    public static final String NAME = "secret";

    public static UseSecret from(final Object config) {

        return DEFAULT;
    }

    @Override
    public Object toConfig(final boolean optional) {

        return Use.name(NAME, optional);
    }

    @Override
    public <R> R visit(final Visitor<R> visitor) {

        return visitor.visitSecret(this);
    }

    @Override
    public Secret create(final Object value, final Set<Name> expand, final boolean suppress) {

        if(value instanceof Secret) {
            return (Secret)value;
        } else {
            if(suppress) {
                return null;
            } else {
                throw new TypeConversionException(Secret.class, "<redacted>");
            }
        }
    }

    @Override
    public Code code() {

        return Code.SECRET;
    }

    @Override
    public Type javaType(final Name name) {

        return Secret.class;
    }

    @Override
    public Secret defaultValue() {

        return Secret.empty();
    }

    @Override
    public void serializeValue(final Secret value, final DataOutput out) throws IOException {

        final byte[] encrypted = value.encrypted();
        out.writeInt(encrypted.length);
        out.write(encrypted);
    }

    @Override
    public Secret deserializeValue(final DataInput in) throws IOException {

        final int length = in.readInt();
        final byte[] encrypted = new byte[length];
        return new Secret(encrypted);
    }

    @Override
    public Schema<?> openApi(final Set<Name> expand) {

        return new StringSchema();
    }

    @Override
    public String toString() {

        return NAME;
    }

    @Override
    public String toString(final Secret value) {

        return value.toString();
    }
}

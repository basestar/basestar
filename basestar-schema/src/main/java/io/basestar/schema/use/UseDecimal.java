package io.basestar.schema.use;

import io.basestar.expression.type.Coercion;
import io.basestar.schema.util.ValueContext;
import io.basestar.util.Name;
import io.basestar.util.Nullsafe;
import io.swagger.v3.oas.models.media.StringSchema;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.util.Map;
import java.util.Set;

@Data
@Slf4j
@RequiredArgsConstructor
public class UseDecimal implements UseNumeric<BigDecimal> {

    public static final UseDecimal DEFAULT = new UseDecimal();

    public static final String PRECISION_KEY = "precision";

    public static final String SCALE_KEY = "scale";

    public static final String NAME = "decimal";

    public static final int DEFAULT_PRECISION = 38;

    public static final int DEFAULT_SCALE = 6;

    private final int precision;

    private final int scale;

    public UseDecimal() {

        this(DEFAULT_PRECISION, DEFAULT_SCALE);
    }

    public static UseDecimal from(final Object config) {

        final int precision;
        final int scale;
        if(config instanceof Map) {
            final Map<?, ?> map = ((Map<?, ?>) config);
            precision = Nullsafe.mapOrDefault(map.get(PRECISION_KEY), Coercion::toInteger, DEFAULT_PRECISION).intValue();
            scale = Nullsafe.mapOrDefault(map.get(SCALE_KEY), Coercion::toInteger, DEFAULT_SCALE).intValue();
        } else {
            precision = DEFAULT_PRECISION;
            scale = DEFAULT_SCALE;
        }
        if(precision == DEFAULT_PRECISION && scale == DEFAULT_SCALE) {
            return UseDecimal.DEFAULT;
        } else {
            return new UseDecimal(precision, scale);
        }
    }

    @Override
    public <R> R visit(final Visitor<R> visitor) {

        return visitor.visitDecimal(this);
    }

    @Override
    public Object toConfig(final boolean optional) {

        return Use.name(NAME, optional);
    }

    @Override
    public BigDecimal create(final ValueContext context, final Object value, final Set<Name> expand) {

        return context.createDecimal(this, value, expand);
    }

    @Override
    public Code code() {

        return Code.DECIMAL;
    }

    @Override
    public Type javaType(final Name name) {

        return BigDecimal.class;
    }

    @Override
    public BigDecimal defaultValue() {

        return BigDecimal.ZERO;
    }

    @Override
    public io.swagger.v3.oas.models.media.Schema<?> openApi(final Set<Name> expand) {

        return new StringSchema();
    }

    @Override
    public void serializeValue(final BigDecimal value, final DataOutput out) throws IOException {

        final String str = value.toString();
        UseString.DEFAULT.serializeValue(str, out);
    }

    @Override
    public BigDecimal deserializeValue(final DataInput in) throws IOException {

        final String str = UseString.DEFAULT.deserializeValue(in);
        return new BigDecimal(str);
    }

    @Override
    public String toString() {

        return NAME;
    }
}

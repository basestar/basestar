package io.basestar.graphql.wiring;

import graphql.schema.Coercing;
import io.basestar.expression.type.Coercion;
import io.basestar.graphql.GraphQLUtils;
import io.basestar.schema.use.UseDecimal;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.Map;

public class DecimalCoercing implements Coercing<BigDecimal, Object> {

    public static final DecimalCoercing INSTANCE = new DecimalCoercing();

    @Override
    public Object serialize(final Object o) {

        if (o instanceof BigDecimal) {
            return UseDecimal.DEFAULT.toString((BigDecimal) o);
        } else {
            throw new IllegalStateException("Expected decimal");
        }
    }

    @Override
    public BigDecimal parseValue(final Object o) {

        return Coercion.toDecimal(o);
    }

    @Override
    public BigDecimal parseLiteral(final Object input) {

        return parseLiteral(input, Collections.emptyMap());
    }

    @Override
    public BigDecimal parseLiteral(final Object input, final Map<String, Object> variables) {

        return parseValue(GraphQLUtils.fromValue(input, variables));
    }
}

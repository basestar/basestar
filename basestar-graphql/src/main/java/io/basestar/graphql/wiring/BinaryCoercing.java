package io.basestar.graphql.wiring;

import graphql.schema.Coercing;
import io.basestar.expression.type.Coercion;
import io.basestar.graphql.GraphQLUtils;
import io.basestar.schema.use.UseBinary;
import io.basestar.util.Bytes;

import java.util.Collections;
import java.util.Map;

public class BinaryCoercing implements Coercing<Bytes, String> {

    public static final BinaryCoercing INSTANCE = new BinaryCoercing();

    @Override
    public String serialize(final Object o) {

        if(o instanceof Bytes) {
            return UseBinary.DEFAULT.toString((Bytes) o);
        } else {
            throw new IllegalStateException("Expected byte array");
        }
    }

    @Override
    public Bytes parseValue(final Object o) {

        return Coercion.toBinary(o);
    }

    @Override
    public Bytes parseLiteral(final Object input) {

        return parseLiteral(input, Collections.emptyMap());
    }

    @Override
    public Bytes parseLiteral(final Object input, final Map<String, Object> variables) {

        return parseValue(GraphQLUtils.fromValue(input, variables));
    }
}

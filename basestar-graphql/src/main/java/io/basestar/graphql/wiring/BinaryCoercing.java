package io.basestar.graphql.wiring;

import graphql.schema.Coercing;
import io.basestar.expression.type.Coercion;
import io.basestar.graphql.GraphQLUtils;
import io.basestar.schema.use.UseBinary;

import java.util.Collections;
import java.util.Map;

public class BinaryCoercing implements Coercing<byte[], String> {

    public static final BinaryCoercing INSTANCE = new BinaryCoercing();

    @Override
    public String serialize(final Object o) {

        if(o instanceof byte[]) {
            return UseBinary.DEFAULT.toString((byte[]) o);
        } else {
            throw new IllegalStateException("Expected byte array");
        }
    }

    @Override
    public byte[] parseValue(final Object o) {

        return Coercion.toBinary(o);
    }

    @Override
    public byte[] parseLiteral(final Object input) {

        return parseLiteral(input, Collections.emptyMap());
    }

    @Override
    public byte[] parseLiteral(final Object input, final Map<String, Object> variables) {

        return parseValue(GraphQLUtils.fromValue(input, variables));
    }
}

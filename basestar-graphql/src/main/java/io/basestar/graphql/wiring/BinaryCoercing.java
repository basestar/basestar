package io.basestar.graphql.wiring;

import graphql.schema.Coercing;
import graphql.schema.CoercingParseLiteralException;
import graphql.schema.CoercingParseValueException;
import graphql.schema.CoercingSerializeException;
import io.basestar.expression.type.Values;
import io.basestar.graphql.GraphQLUtils;
import io.basestar.schema.use.UseBinary;

import java.util.Collections;
import java.util.Map;

public class BinaryCoercing implements Coercing<byte[], String> {

    public static BinaryCoercing INSTANCE = new BinaryCoercing();

    @Override
    public String serialize(final Object o) throws CoercingSerializeException {

        if(o instanceof byte[]) {
            return UseBinary.DEFAULT.toString((byte[]) o);
        } else {
            throw new IllegalStateException("Expected byte array");
        }
    }

    @Override
    public byte[] parseValue(final Object o) throws CoercingParseValueException {

        return Values.toBinary(o);
    }

    @Override
    public byte[] parseLiteral(final Object input) throws CoercingParseLiteralException {

        return parseLiteral(input, Collections.emptyMap());
    }

    @Override
    public byte[] parseLiteral(final Object input, final Map<String, Object> variables) throws CoercingParseLiteralException {

        return parseValue(GraphQLUtils.fromValue(input, variables));
    }
}

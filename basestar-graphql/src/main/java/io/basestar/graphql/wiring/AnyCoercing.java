package io.basestar.graphql.wiring;

import graphql.schema.Coercing;
import graphql.schema.CoercingParseLiteralException;
import graphql.schema.CoercingParseValueException;
import graphql.schema.CoercingSerializeException;
import io.basestar.graphql.GraphQLUtils;

import java.util.Collections;
import java.util.Map;

public class AnyCoercing implements Coercing<Object, Object> {

    @Override
    public Object serialize(final Object input) throws CoercingSerializeException {

        return input;
    }

    @Override
    public Object parseValue(final Object input) throws CoercingParseValueException {

        return GraphQLUtils.fromValue(input, Collections.emptyMap());
    }

    @Override
    public Object parseLiteral(final Object input) throws CoercingParseLiteralException {

        return parseLiteral(input, Collections.emptyMap());
    }

    @Override
    public Object parseLiteral(final Object input, final Map<String, Object> variables) throws CoercingParseLiteralException {

        return GraphQLUtils.fromValue(input, variables);
    }
}

package io.basestar.graphql.wiring;

import graphql.schema.Coercing;
import io.basestar.graphql.GraphQLUtils;

import java.util.Collections;
import java.util.Map;

public class AnyCoercing implements Coercing<Object, Object> {

    public static final AnyCoercing INSTANCE = new AnyCoercing();

    @Override
    public Object serialize(final Object input) {

        return input;
    }

    @Override
    public Object parseValue(final Object input) {

        return input;
    }

    @Override
    public Object parseLiteral(final Object input) {

        return parseLiteral(input, Collections.emptyMap());
    }

    @Override
    public Object parseLiteral(final Object input, final Map<String, Object> variables) {

        return parseValue(GraphQLUtils.fromValue(input, variables));
    }
}

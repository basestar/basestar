package io.basestar.graphql.wiring;

import graphql.schema.Coercing;
import graphql.schema.CoercingParseLiteralException;
import graphql.schema.CoercingParseValueException;
import graphql.schema.CoercingSerializeException;
import io.basestar.graphql.GraphQLUtils;
import io.basestar.util.ISO8601;

import java.time.Instant;
import java.util.Collections;
import java.util.Map;

public class DateTimeCoercing implements Coercing<Instant, String> {

    public static DateTimeCoercing INSTANCE = new DateTimeCoercing();

    @Override
    public String serialize(final Object o) throws CoercingSerializeException {

        final Instant value = ISO8601.toDateTime(o);
        return value == null ? null : ISO8601.toString(value);
    }

    @Override
    public Instant parseValue(final Object o) throws CoercingParseValueException {

        return ISO8601.toDateTime(o);
    }

    @Override
    public Instant parseLiteral(final Object input) throws CoercingParseLiteralException {

        return parseLiteral(input, Collections.emptyMap());
    }

    @Override
    public Instant parseLiteral(final Object input, final Map<String, Object> variables) throws CoercingParseLiteralException {

        return parseValue(GraphQLUtils.fromValue(input, variables));
    }
}

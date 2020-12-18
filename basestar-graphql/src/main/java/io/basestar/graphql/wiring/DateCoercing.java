package io.basestar.graphql.wiring;

import graphql.schema.Coercing;
import graphql.schema.CoercingParseLiteralException;
import graphql.schema.CoercingParseValueException;
import graphql.schema.CoercingSerializeException;
import io.basestar.graphql.GraphQLUtils;
import io.basestar.util.ISO8601;

import java.time.LocalDate;
import java.util.Collections;
import java.util.Map;

public class DateCoercing implements Coercing<LocalDate, String> {

    public static DateCoercing INSTANCE = new DateCoercing();

    @Override
    public String serialize(final Object o) throws CoercingSerializeException {

        final LocalDate value = ISO8601.toDate(o);
        return value == null ? null : ISO8601.toString(value);
    }

    @Override
    public LocalDate parseValue(final Object o) throws CoercingParseValueException {

        return ISO8601.toDate(o);
    }

    @Override
    public LocalDate parseLiteral(final Object input) throws CoercingParseLiteralException {

        return parseLiteral(input, Collections.emptyMap());
    }

    @Override
    public LocalDate parseLiteral(final Object input, final Map<String, Object> variables) throws CoercingParseLiteralException {

        return parseValue(GraphQLUtils.fromValue(input, variables));
    }
}

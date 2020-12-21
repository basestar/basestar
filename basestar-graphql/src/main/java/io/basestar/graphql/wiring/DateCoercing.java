package io.basestar.graphql.wiring;

import graphql.schema.Coercing;
import io.basestar.graphql.GraphQLUtils;
import io.basestar.util.ISO8601;

import java.time.LocalDate;
import java.util.Collections;
import java.util.Map;

public class DateCoercing implements Coercing<LocalDate, String> {

    public static final DateCoercing INSTANCE = new DateCoercing();

    @Override
    public String serialize(final Object o) {

        final LocalDate value = ISO8601.toDate(o);
        return value == null ? null : ISO8601.toString(value);
    }

    @Override
    public LocalDate parseValue(final Object o) {

        return ISO8601.toDate(o);
    }

    @Override
    public LocalDate parseLiteral(final Object input) {

        return parseLiteral(input, Collections.emptyMap());
    }

    @Override
    public LocalDate parseLiteral(final Object input, final Map<String, Object> variables) {

        return parseValue(GraphQLUtils.fromValue(input, variables));
    }
}

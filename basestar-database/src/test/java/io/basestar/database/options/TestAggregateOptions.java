package io.basestar.database.options;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.expression.methods.Methods;
import io.basestar.schema.aggregate.Sum;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestAggregateOptions {

    @Test
    public void parseGroup() throws IOException {

        final ObjectMapper objectMapper = new ObjectMapper();

        assertEquals(
                AggregateOptions.builder().setGroup(ImmutableMap.of("a", expr("a"))).build(),
                objectMapper.readValue(json("{'group': 'a'}"), AggregateOptions.class)
        );

        assertEquals(
                AggregateOptions.builder().setGroup(ImmutableMap.of("a", expr("a"), "b", expr("b"))).build(),
                objectMapper.readValue(json("{'group': ['a', 'b']}"), AggregateOptions.class)
        );

        assertEquals(
                AggregateOptions.builder().setGroup(ImmutableMap.of("a", expr("1 + 2"), "b", expr("c * d"))).build(),
                objectMapper.readValue(json("{'group': {'a': '1 + 2', 'b': 'c * d'}}"), AggregateOptions.class)
        );
    }

    @Test
    public void parseAggregate() throws IOException {

        final ObjectMapper objectMapper = new ObjectMapper();

        assertEquals(
                AggregateOptions.builder().setAggregate(ImmutableMap.of("test", Sum.builder().setInput(expr("x")).build())).build(),
                objectMapper.readValue(json("{'aggregate': { 'test': { 'function': 'sum', 'input': 'x' } } }"), AggregateOptions.class)
        );
    }

    // Allow shorthand json with single quotes
    private String json(final String str) {

        return str.replaceAll("'", "\"");
    }

    private Expression expr(final String str) {

        final Expression expr = Expression.parse(str);
        return expr.bind(Context.init(Methods.builder().build()));
    }
}

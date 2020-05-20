package io.basestar.database.options;

/*-
 * #%L
 * basestar-database
 * %%
 * Copyright (C) 2019 - 2020 Basestar.IO
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

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

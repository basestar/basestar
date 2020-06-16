package io.basestar.schema.serde;

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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.google.common.collect.ImmutableMap;
import io.basestar.expression.Expression;
import io.basestar.jackson.serde.ExpressionDeseriaizer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ViewGroupDeserializer extends JsonDeserializer<Map<String, Expression>> {

    private final JsonDeserializer<Expression> expressionDeserializer;

    public ViewGroupDeserializer() {

        this(new ExpressionDeseriaizer());
    }

    public ViewGroupDeserializer(final JsonDeserializer<Expression> expressionDeserializer) {

        this.expressionDeserializer = expressionDeserializer;
    }

    @Override
    public Map<String, Expression> deserialize(final JsonParser jsonParser, final DeserializationContext deserializationContext) throws IOException {

        switch (jsonParser.getCurrentToken()) {
            case VALUE_STRING: {
                final Expression expr = expressionDeserializer.deserialize(jsonParser, deserializationContext);
                return ImmutableMap.of(expr.toString(), expr);
            }
            case START_ARRAY: {
                final Map<String, Expression> exprs = new HashMap<>();
                jsonParser.nextToken();
                while(jsonParser.getCurrentToken() != JsonToken.END_ARRAY) {
                    final Expression expr = expressionDeserializer.deserialize(jsonParser, deserializationContext);
                    exprs.put(expr.toString(), expr);
                    jsonParser.nextToken();
                }
                return exprs;
            }
            case START_OBJECT: {
                final Map<String, Expression> exprs = new HashMap<>();
                jsonParser.nextToken();
                while(jsonParser.getCurrentToken() != JsonToken.END_OBJECT) {
                    if(jsonParser.getCurrentToken() == JsonToken.FIELD_NAME) {
                        final String name = jsonParser.getCurrentName();
                        jsonParser.nextToken();
                        final Expression expr = expressionDeserializer.deserialize(jsonParser, deserializationContext);
                        exprs.put(name, expr);
                        jsonParser.nextToken();
                    } else {
                        throw deserializationContext.wrongTokenException(jsonParser, Map.class, JsonToken.FIELD_NAME, null);
                    }
                }
                return exprs;
            }
            default:
                throw deserializationContext.wrongTokenException(jsonParser, Map.class, JsonToken.START_OBJECT, null);
        }
    }
}

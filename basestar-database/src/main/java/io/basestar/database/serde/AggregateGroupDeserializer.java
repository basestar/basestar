package io.basestar.database.serde;

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

public class AggregateGroupDeserializer extends JsonDeserializer<Map<String, Expression>> {

    private final JsonDeserializer<Expression> expressionDeserializer;

    public AggregateGroupDeserializer() {

        this(new ExpressionDeseriaizer());
    }

    public AggregateGroupDeserializer(final JsonDeserializer<Expression> expressionDeserializer) {

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

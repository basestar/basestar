package io.basestar.jackson.serde;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.expression.methods.Methods;

import java.io.IOException;

public class ExpressionDeseriaizer extends JsonDeserializer<Expression> {

    @Override
    public Expression deserialize(final JsonParser jsonParser, final DeserializationContext deserializationContext) throws IOException {

        final Expression expr = Expression.parse(jsonParser.getText());
        return expr.bind(Context.init(Methods.builder().build()));
    }
}

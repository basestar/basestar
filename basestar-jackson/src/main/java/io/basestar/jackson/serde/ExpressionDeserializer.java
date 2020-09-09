package io.basestar.jackson.serde;

/*-
 * #%L
 * basestar-jackson
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

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.expression.constant.Constant;
import io.basestar.expression.literal.LiteralArray;
import io.basestar.expression.literal.LiteralObject;
import io.basestar.expression.methods.Methods;

import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;

public class ExpressionDeserializer extends JsonDeserializer<Expression> {

    @Override
    public Expression deserialize(final JsonParser jsonParser, final DeserializationContext deserializationContext) throws IOException {

        final Expression expr = parse(jsonParser);
        return expr.bind(Context.init(Methods.builder().build()));
    }

    protected Expression parse(final JsonParser jsonParser)  throws IOException {

        final JsonToken token = jsonParser.getCurrentToken();
        switch(token) {
            case START_ARRAY:
                return new LiteralArray(jsonParser.readValueAs(Expression[].class));
            case START_OBJECT:
            case VALUE_EMBEDDED_OBJECT: {
                final Map<String, Expression> exprs = jsonParser.readValueAs(new TypeReference<Map<String, Expression>>() {});
                return new LiteralObject(exprs.entrySet().stream().collect(Collectors.toMap(
                        e -> new Constant(e.getKey()),
                        Map.Entry::getValue
                )));
            }
            case VALUE_NUMBER_FLOAT:
                return new Constant(jsonParser.getDoubleValue());
            case VALUE_NUMBER_INT:
                return new Constant(jsonParser.getLongValue());
            case VALUE_STRING:
                return Expression.parse(jsonParser.getText());
            case VALUE_TRUE:
                return new Constant(true);
            case VALUE_FALSE:
                return new Constant(false);
            default:
                throw new JsonParseException(jsonParser, "Cannot parse expression");
        }
    }
}

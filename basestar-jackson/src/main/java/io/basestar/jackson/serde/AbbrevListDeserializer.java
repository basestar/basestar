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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.deser.ContextualDeserializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class AbbrevListDeserializer<T> extends JsonDeserializer<List<T>> implements ContextualDeserializer {

    private final Class<T> type;

    private AbbrevListDeserializer() {

        this.type = null;
    }

    private AbbrevListDeserializer(final Class<T> type) {

        this.type = type;
    }

    public List<T> deserialize(final JsonParser jsonParser, final DeserializationContext deserializationContext) throws IOException {

        assert type != null;
        JsonToken next = jsonParser.currentToken();
        if (next == JsonToken.START_ARRAY) {
            final List<T> results = new ArrayList<>();
            next = jsonParser.nextToken();
            while (next != JsonToken.END_ARRAY) {
                results.add(jsonParser.readValueAs(type));
                next = jsonParser.nextToken();
            }
            return results;
        } else {
            return Collections.singletonList(jsonParser.readValueAs(type));
        }
    }

    @SuppressWarnings("unchecked")
    public JsonDeserializer<?> createContextual(final DeserializationContext deserializationContext, final BeanProperty beanProperty) {

        final JavaType type = beanProperty.getType();
        return new AbbrevListDeserializer(type.containedType(0).getRawClass());
    }
}

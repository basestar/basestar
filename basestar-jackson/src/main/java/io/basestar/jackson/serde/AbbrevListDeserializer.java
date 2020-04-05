package io.basestar.jackson.serde;

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

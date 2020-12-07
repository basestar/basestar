package io.basestar.jackson.serde;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

import java.io.IOException;
import java.io.Serializable;

public class SerializableDeserializer extends JsonDeserializer<Serializable> {

    @Override
    public Serializable deserialize(final JsonParser jsonParser, final DeserializationContext deserializationContext) throws IOException {

        return (Serializable)jsonParser.readValueAs(Object.class);
    }
}

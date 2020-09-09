package io.basestar.jackson.serde;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import io.basestar.util.ISO8601;

import java.io.IOException;
import java.time.Instant;

public class DateTimeDeserializer extends JsonDeserializer<Instant> {
    
    @Override
    public Instant deserialize(final JsonParser jsonParser, final DeserializationContext deserializationContext) throws IOException {
        
        return ISO8601.toDateTime(jsonParser.readValuesAs(Object.class));
    }
}

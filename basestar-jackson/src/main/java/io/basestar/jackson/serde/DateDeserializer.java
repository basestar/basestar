package io.basestar.jackson.serde;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import io.basestar.util.ISO8601;

import java.io.IOException;
import java.time.LocalDate;

public class DateDeserializer extends JsonDeserializer<LocalDate> {

    @Override
    public LocalDate deserialize(final JsonParser jsonParser, final DeserializationContext deserializationContext) throws IOException {

        return ISO8601.toDate(jsonParser.readValuesAs(Object.class));
    }
}

package io.basestar.jackson.serde;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import io.basestar.util.ISO8601;

import java.io.IOException;
import java.time.temporal.TemporalAccessor;

public class TemporalAccessorSerializer extends JsonSerializer<TemporalAccessor> {

    @Override
    public void serialize(final TemporalAccessor value, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider) throws IOException {

        jsonGenerator.writeString(ISO8601.toString(value));
    }
}

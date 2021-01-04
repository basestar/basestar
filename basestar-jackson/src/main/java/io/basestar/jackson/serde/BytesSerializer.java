package io.basestar.jackson.serde;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import io.basestar.util.Bytes;

import java.io.IOException;

public class BytesSerializer extends JsonSerializer<Bytes> {

    @Override
    public void serialize(final Bytes value, final JsonGenerator generator, final SerializerProvider provider) throws IOException {

        generator.writeString(value.toString());
    }
}

package io.basestar.jackson.serde;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import io.basestar.util.Bytes;

import java.io.IOException;

public class BytesDeserializer extends JsonDeserializer<Bytes> {

    @Override
    public Bytes deserialize(final JsonParser parser, final DeserializationContext context) throws IOException, JsonProcessingException {

        final String str = parser.getValueAsString();
        return Bytes.fromBase64(str);
    }
}

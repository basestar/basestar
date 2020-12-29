package io.basestar.jackson.serde;

import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.KeyDeserializer;
import io.basestar.util.ISO8601;

public class LocalDateKeyDeserializer extends KeyDeserializer {

    @Override
    public Object deserializeKey(final String str, final DeserializationContext deserializationContext) {

        return ISO8601.parseDate(str);
    }
}

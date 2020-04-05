package io.basestar.jackson.serde;

import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.KeyDeserializer;
import io.basestar.util.Path;

public class PathKeyDeserializer extends KeyDeserializer {

    @Override
    public Object deserializeKey(final String s, final DeserializationContext deserializationContext) {

        return Path.parse(s);
    }
}

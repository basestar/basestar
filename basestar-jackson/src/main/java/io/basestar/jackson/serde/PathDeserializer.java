package io.basestar.jackson.serde;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import io.basestar.util.Path;

import java.io.IOException;

public class PathDeserializer extends JsonDeserializer<Path> {

    @Override
    public Path deserialize(final JsonParser jsonParser, final DeserializationContext deserializationContext) throws IOException {

        return Path.parse(jsonParser.getText());
    }
}

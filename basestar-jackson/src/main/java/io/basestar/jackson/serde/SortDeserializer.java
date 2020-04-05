package io.basestar.jackson.serde;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import io.basestar.util.Sort;

import java.io.IOException;

public class SortDeserializer extends JsonDeserializer<Sort> {

    @Override
    public Sort deserialize(final JsonParser jsonParser, final DeserializationContext deserializationContext) throws IOException {

        return Sort.parse(jsonParser.getText());
    }
}

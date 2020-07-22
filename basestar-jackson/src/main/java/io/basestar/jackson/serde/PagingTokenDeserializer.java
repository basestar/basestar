package io.basestar.jackson.serde;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import io.basestar.util.PagingToken;

import java.io.IOException;

public class PagingTokenDeserializer extends JsonDeserializer<PagingToken> {

    @Override
    public PagingToken deserialize(final JsonParser jsonParser, final DeserializationContext deserializationContext) throws IOException {

        final String str = jsonParser.getText();
        return str == null ? null : new PagingToken(str);
    }
}

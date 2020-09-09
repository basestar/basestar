package io.basestar.jackson.serde;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import io.basestar.util.Page;

import java.io.IOException;

public class PagingTokenDeserializer extends JsonDeserializer<Page.Token> {

    @Override
    public Page.Token deserialize(final JsonParser jsonParser, final DeserializationContext deserializationContext) throws IOException {

        final String str = jsonParser.getText();
        return str == null ? null : new Page.Token(str);
    }
}

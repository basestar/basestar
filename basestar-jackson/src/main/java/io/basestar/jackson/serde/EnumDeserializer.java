package io.basestar.jackson.serde;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.deser.ContextualDeserializer;
import lombok.AllArgsConstructor;

import java.io.IOException;

@AllArgsConstructor
@SuppressWarnings({"rawtypes", "unchecked"})
public class EnumDeserializer extends JsonDeserializer<Enum> implements ContextualDeserializer {

    private final Class<? extends Enum> type;

    public EnumDeserializer() {

        this(null);
    }

    @Override
    public Enum deserialize(final JsonParser parser, final DeserializationContext context) throws IOException, JsonProcessingException {

        assert type != null;
        final String str = parser.getValueAsString();
        return Enum.valueOf(type, str.replaceAll("-", "_").toUpperCase());
    }

    @Override
    public EnumDeserializer createContextual(final DeserializationContext context, final BeanProperty property) {

        return new EnumDeserializer((Class<? extends Enum>)context.getContextualType().getRawClass());
    }
}

package io.basestar.jackson.serde;

import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.KeyDeserializer;
import com.fasterxml.jackson.databind.deser.ContextualKeyDeserializer;
import lombok.AllArgsConstructor;

@AllArgsConstructor
@SuppressWarnings({"rawtypes", "unchecked"})
public class EnumKeyDeserializer extends KeyDeserializer implements ContextualKeyDeserializer {

    private final Class<? extends Enum> type;

    public EnumKeyDeserializer() {

        this(null);
    }

    @Override
    public Object deserializeKey(final String str, final DeserializationContext deserializationContext) {

        assert type != null;
        return Enum.valueOf(type, str.replaceAll("-", "_").toUpperCase());
    }

    @Override
    public KeyDeserializer createContextual(final DeserializationContext context, final BeanProperty property) throws JsonMappingException {

        return new EnumKeyDeserializer((Class<? extends Enum>)context.getContextualType().containedType(0).getRawClass());
    }
}

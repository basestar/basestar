package io.basestar.mapper.jackson;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.deser.ContextualDeserializer;
import io.basestar.mapper.MappingContext;
import io.basestar.mapper.SchemaMapper;

import java.io.IOException;

public class MarshallingDeserializer<T> extends JsonDeserializer<T> implements ContextualDeserializer {

    private final SchemaMapper<T, Object> mapper;

    public MarshallingDeserializer() {

        this.mapper = null;
    }

    public MarshallingDeserializer(final Class<T> of) {

        final MappingContext mappingContext = new MappingContext();
        this.mapper = mappingContext.schemaMapper(of);
    }

    @Override
    public T deserialize(final JsonParser jsonParser, final DeserializationContext context) throws IOException {

        if(mapper == null) {
            throw new JsonMappingException(null, "No mapping type available", jsonParser.getCurrentLocation());
        } else {
            return mapper.marshall(jsonParser.readValueAs(Object.class));
        }
    }

    @Override
    public JsonDeserializer<?> createContextual(final DeserializationContext context, final BeanProperty property) {

        if(property != null) {
            final Class<?> of = property.getType().getRawClass();
            return new MarshallingDeserializer<>(of);
        } else if(context.getContextualType() != null) {
            final Class<?> of = context.getContextualType().getRawClass();
            return new MarshallingDeserializer<>(of);
        } else {
            return new MarshallingDeserializer<>();
        }
    }
}

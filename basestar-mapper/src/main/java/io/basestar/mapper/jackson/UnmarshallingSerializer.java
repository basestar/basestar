package io.basestar.mapper.jackson;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import io.basestar.mapper.MappingContext;
import io.basestar.mapper.SchemaMapper;

import java.io.IOException;

public class UnmarshallingSerializer<T> extends JsonSerializer<T> {

    final MappingContext mappingContext = new MappingContext();

    @Override
    public void serialize(final T value, final JsonGenerator generator, final SerializerProvider provider) throws IOException {

        @SuppressWarnings("unchecked")
        final SchemaMapper<T, Object> mapper = mappingContext.schemaMapper((Class<T>)value.getClass());
        generator.writeObject(mapper.unmarshall(value));
    }
}

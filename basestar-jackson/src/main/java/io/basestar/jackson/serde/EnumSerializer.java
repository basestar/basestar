package io.basestar.jackson.serde;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;

@SuppressWarnings("rawtypes")
public class EnumSerializer extends JsonSerializer<Enum> {

    @Override
    public void serialize(final Enum value, final JsonGenerator generator, final SerializerProvider provider) throws IOException {

        generator.writeString(value.toString().toLowerCase().replaceAll("_", "-"));
    }
}

package io.basestar.schema;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;

import java.io.IOException;
import java.io.Serializable;

public interface Named extends Serializable {

    String getName();

    class NameSerializer extends JsonSerializer<Named> {

        @Override
        public void serialize(final Named named, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider) throws IOException {

            jsonGenerator.writeString(named.getName());
        }

        @Override
        public void serializeWithType(final Named named, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider, final TypeSerializer typeSerializer) throws IOException {

            serialize(named, jsonGenerator, serializerProvider);
        }
    }
}

package io.basestar.jackson.serde;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.google.common.io.BaseEncoding;
import io.basestar.secret.Secret;
import lombok.RequiredArgsConstructor;

import java.io.IOException;

@RequiredArgsConstructor
public class SecretSerializer extends JsonSerializer<Secret> {

    @Override
    public void serialize(final Secret value, final JsonGenerator generator, final SerializerProvider provider) throws IOException {

        if(value instanceof Secret.Encrypted) {
            generator.writeString(BaseEncoding.base64().encode(value.encrypted()));
        } else {
            throw new IllegalStateException("Cannot serialize" + value.getClass());
        }
    }
}

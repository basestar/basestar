package io.basestar.jackson.serde;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.google.common.io.BaseEncoding;
import io.basestar.secret.Secret;
import lombok.RequiredArgsConstructor;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

@RequiredArgsConstructor
public class SecretSerializer extends JsonSerializer<Secret> {

    private final boolean visibleSecrets;

    @Override
    public void serialize(final Secret value, final JsonGenerator generator, final SerializerProvider provider) throws IOException {

        if(visibleSecrets && value instanceof Secret.Plaintext) {
            final String plaintext = new String(((Secret.Plaintext) value).plaintext(), StandardCharsets.UTF_8);
            generator.writeString(plaintext);
        } else if(!visibleSecrets && value instanceof Secret.Encrypted) {
            generator.writeString(BaseEncoding.base64().encode(value.encrypted()));
        } else {
            throw new IllegalStateException("Cannot serialize" + value.getClass() + " (with secret visibility: " + visibleSecrets + ")");
        }
    }
}

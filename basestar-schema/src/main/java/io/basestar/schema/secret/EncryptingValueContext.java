package io.basestar.schema.secret;

import io.basestar.expression.type.exception.TypeConversionException;
import io.basestar.schema.use.UseSecret;
import io.basestar.schema.use.ValueContext;
import io.basestar.util.Name;

import java.nio.charset.StandardCharsets;
import java.util.Set;

public class EncryptingValueContext extends ValueContext.Standard {

    private final SecretContext secretContext;

    public EncryptingValueContext(final SecretContext secretContext) {

        this.secretContext = secretContext;
    }

    @Override
    public Secret createSecret(final UseSecret secret, final Object value, final Set<Name> expand) {

        if(value instanceof String) {
            return secretContext.encrypt(((String) value).getBytes(StandardCharsets.UTF_8));
        } else {
            throw new TypeConversionException(Secret.class, "<redacted>");
        }
    }
}

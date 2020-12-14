package io.basestar.graphql.wiring;

import graphql.schema.Coercing;
import graphql.schema.CoercingParseLiteralException;
import graphql.schema.CoercingParseValueException;
import graphql.schema.CoercingSerializeException;
import io.basestar.graphql.GraphQLUtils;
import io.basestar.secret.Secret;
import io.basestar.secret.SecretContext;
import lombok.RequiredArgsConstructor;

import java.util.Collections;
import java.util.Map;

@RequiredArgsConstructor
public class SecretCoercing implements Coercing<Secret, String> {

    private final SecretContext secretContext;

    @Override
    public String serialize(final Object o) throws CoercingSerializeException {

        if(o instanceof Secret.Encrypted) {
            return secretContext.decrypt(((Secret.Encrypted) o))
                    .join().plaintextString();
        } else {
            throw new IllegalStateException("Expected encrypted secret, got: " + o);
        }
    }

    @Override
    public Secret parseValue(final Object o) throws CoercingParseValueException {

        if(o instanceof String) {
            return secretContext.encrypt(Secret.plaintext((String)o)).join();
        } else {
            throw new IllegalStateException("Expected plaintext secret, got: " + o);
        }
    }

    @Override
    public Secret parseLiteral(final Object input) throws CoercingParseLiteralException {

        return parseLiteral(input, Collections.emptyMap());
    }

    @Override
    public Secret parseLiteral(final Object input, final Map<String, Object> variables) throws CoercingParseLiteralException {

        return parseValue(GraphQLUtils.fromValue(input, variables));
    }
}

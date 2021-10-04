package io.basestar.auth;

import io.basestar.auth.exception.AuthenticationFailedException;
import io.swagger.v3.oas.models.security.SecurityScheme;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;

class TestBasicAuthenticator {

    @Test
    void testAuthenticate() {

        final Authenticator authenticator = new BasicAuthenticator() {
            @Override
            protected CompletableFuture<Caller> verify(final String username, final String password) {

                if ("super".equals(username) && "password".equals(password)) {
                    return CompletableFuture.completedFuture(Caller.SUPER);
                } else {
                    throw new AuthenticationFailedException("Authentication failed (invalid username/password)");
                }
            }
        };

        final Caller caller = authenticator.authenticate(BasicAuthenticator.authorization("super", "password")).join();
        assertTrue(caller.isSuper());

        assertThrows(AuthenticationFailedException.class, () -> authenticator.authenticate(BasicAuthenticator.authorization("super", "badpass")));
        assertThrows(AuthenticationFailedException.class, () -> authenticator.authenticate(BasicAuthenticator.authorization("baduser", "password")));
        assertThrows(AuthenticationFailedException.class, () -> authenticator.authenticate(Authorization.from("Basic invalid")));

        final Map<String, SecurityScheme> schemes = authenticator.openApi();
        final SecurityScheme scheme = schemes.get("Basic");
        assertEquals(SecurityScheme.Type.HTTP, scheme.getType());
        assertEquals(SecurityScheme.In.HEADER, scheme.getIn());
    }
}

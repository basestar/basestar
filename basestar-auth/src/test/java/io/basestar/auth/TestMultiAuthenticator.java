package io.basestar.auth;

import com.google.common.collect.ImmutableList;
import io.swagger.v3.oas.models.security.SecurityScheme;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestMultiAuthenticator {

    @Test
    void testAuthenticate() {

        final Authenticator authenticator = new MultiAuthenticator(ImmutableList.of(
                new BasicAuthenticator() {
                    @Override
                    protected CompletableFuture<Caller> verify(final String username, final String password) {

                        return CompletableFuture.completedFuture(Caller.builder().setId(username).build());
                    }
                },
                new StaticAuthenticator(Caller.ANON)
        ));

        final Caller user = authenticator.authenticate(BasicAuthenticator.authorization("user1", "password")).join();
        assertEquals("user1", user.getId());

        final Caller fallback = authenticator.authenticate(Authorization.of("Unknown", "unknown")).join();
        assertTrue(fallback.isAnon());

        final Map<String, SecurityScheme> schemes = authenticator.openApi();
        assertTrue(schemes.containsKey("Basic"));
    }

}

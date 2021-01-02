package io.basestar.auth;

import io.swagger.v3.oas.models.security.SecurityScheme;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class TestExplicitAuthenticator {

    @Test
    void testAuthenticate() {

        final Authenticator authenticator = new ExplicitAuthenticator();

        final Caller user = authenticator.authenticate(authorization("user1")).join();
        assertFalse(user.isSuper());
        assertFalse(user.isAnon());
        assertEquals("user1", user.getId());

        final Caller superuser = authenticator.authenticate(authorization("super")).join();
        assertTrue(superuser.isSuper());
        assertFalse(superuser.isAnon());

        final Caller anon = authenticator.authenticate(authorization("anon")).join();
        assertFalse(anon.isSuper());
        assertTrue(anon.isAnon());

        final Map<String, SecurityScheme> schemes = authenticator.openApi();
        assertEquals(Collections.<String, SecurityScheme>emptyMap(), schemes);
    }

    private Authorization authorization(final String username) {

        return Authorization.from("Explicit " + username);
    }
}

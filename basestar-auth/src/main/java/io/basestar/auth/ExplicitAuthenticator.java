package io.basestar.auth;

import com.google.common.collect.ImmutableMap;
import io.swagger.v3.oas.models.security.SecurityScheme;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Warning: this should only be used in a trusted context, e.g. where authentication is provided by an API Gateway
 * and the target is only accessible via that gateway.
 *
 * Usage:
 *
 * <code>Authorization: Explicit {user_id}</code>
 */

public class ExplicitAuthenticator implements Authenticator {

    @Override
    public boolean canAuthenticate(final Authorization auth) {

        return "Explicit".equalsIgnoreCase(auth.getType());
    }

    @Override
    public CompletableFuture<Caller> authenticate(final Authorization auth) {

        final String id = auth.getCredentials();
        return CompletableFuture.completedFuture(new Caller() {
            @Override
            public boolean isAnon() {

                return "anon".equalsIgnoreCase(id);
            }

            @Override
            public boolean isSuper() {

                return "super".equalsIgnoreCase(id);
            }

            @Override
            public String getSchema() {

                return "User";
            }

            @Override
            public String getId() {

                return id;
            }

            @Override
            public Map<String, Object> getClaims() {

                return ImmutableMap.of();
            }
        });
    }

    @Override
    public Map<String, SecurityScheme> openApi() {

        return ImmutableMap.of();
    }
}

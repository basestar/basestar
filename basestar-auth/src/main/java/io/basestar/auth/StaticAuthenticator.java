package io.basestar.auth;

import io.swagger.v3.oas.models.security.SecurityScheme;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class StaticAuthenticator implements Authenticator {

    private final Caller caller;

    public StaticAuthenticator(final Caller caller) {

        this.caller = caller;
    }

    @Override
    public CompletableFuture<Caller> authenticate(final String authorization) {

        return CompletableFuture.completedFuture(caller);
    }

    @Override
    public Map<String, SecurityScheme> openApi() {

        return Collections.emptyMap();
    }
}

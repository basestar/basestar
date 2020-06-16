package io.basestar.auth;

import com.google.common.collect.ImmutableList;
import io.swagger.v3.oas.models.security.SecurityScheme;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class MultiAuthenticator implements Authenticator {

    private final List<Authenticator> authenticators;

    @lombok.Builder(builderClassName = "Builder")
    MultiAuthenticator(final List<Authenticator> authenticators) {

        this.authenticators = ImmutableList.copyOf(authenticators);
    }

    @Override
    public boolean canAuthenticate(final Authorization auth) {

        return authenticators.stream().anyMatch(v -> v.canAuthenticate(auth));
    }

    @Override
    public CompletableFuture<Caller> authenticate(final Authorization auth) {

        for(final Authenticator authenticator : authenticators) {
            if(authenticator.canAuthenticate(auth)) {
                return authenticator.authenticate(auth);
            }
        }
        throw new IllegalStateException("No valid authenticator");
    }

    @Override
    public Map<String, SecurityScheme> openApi() {

        final Map<String, SecurityScheme> schemes = new HashMap<>();
        authenticators.forEach(authenticator -> schemes.putAll(authenticator.openApi()));
        return schemes;
    }
}

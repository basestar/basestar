package io.basestar.auth;

import com.google.common.base.Charsets;
import com.google.common.io.BaseEncoding;
import io.basestar.auth.exception.AuthenticationFailedException;

import java.util.concurrent.CompletableFuture;

public abstract class BasicAuthenticator implements Authenticator {

    @Override
    public boolean canAuthenticate(final Authorization auth) {

        return auth.isBasic();
    }

    @Override
    public CompletableFuture<Caller> authenticate(final Authorization authorization) {

        final String token = authorization.getCredentials();

        final String decoded = new String(BaseEncoding.base64().decode(token), Charsets.UTF_8);
        final String[] creds = decoded.split("\\:");
        if (creds.length == 2) {
            final String username = creds[0];
            final String password = creds[1];

            return verify(username, password);

        } else {
            throw new AuthenticationFailedException("Authorization header did not match declared format");
        }
    }

    protected abstract CompletableFuture<Caller> verify(final String username, final String password);
}
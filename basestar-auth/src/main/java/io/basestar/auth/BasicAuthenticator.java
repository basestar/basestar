package io.basestar.auth;

/*-
 * #%L
 * basestar-auth
 * %%
 * Copyright (C) 2019 - 2020 Basestar.IO
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import io.basestar.auth.exception.AuthenticationFailedException;
import io.swagger.v3.oas.models.security.SecurityScheme;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public abstract class BasicAuthenticator implements Authenticator {

    public static final String TYPE = "Basic";

    @Override
    public boolean canAuthenticate(final Authorization auth) {

        return auth.isBasic();
    }

    @Override
    public CompletableFuture<Caller> authenticate(final Authorization authorization) {

        final String token = authorization.getCredentials();

        final String decoded = new String(Base64.getDecoder().decode(token), Charsets.UTF_8);
        final String[] creds = decoded.split("\\:");
        if (creds.length == 2) {
            final String username = creds[0];
            final String password = creds[1];

            return verify(username, password);

        } else {
            return throwFailure(authorization);
        }
    }

    public static Authorization authorization(final String username, final String password) {

        return Authorization.of(TYPE,
                Base64.getEncoder().encodeToString((username + ":" + password).getBytes(StandardCharsets.UTF_8)));
    }

    protected CompletableFuture<Caller> throwFailure(final Authorization authorization) {

        throw new AuthenticationFailedException("Authorization header did not match declared format");
    }

    protected abstract CompletableFuture<Caller> verify(final String username, final String password);

    @Override
    public Map<String, SecurityScheme> openApi() {

        return ImmutableMap.of("Basic", new SecurityScheme()
                .type(SecurityScheme.Type.HTTP)
                .in(SecurityScheme.In.HEADER)
                .scheme("basic"));
    }
}

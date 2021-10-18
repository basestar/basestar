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

import com.google.common.collect.ImmutableList;
import io.swagger.v3.oas.models.security.SecurityScheme;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@Slf4j
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

        for (final Authenticator authenticator : authenticators) {
            if (authenticator.canAuthenticate(auth)) {
                log.debug("Using authenticator {}", auth.getClass().getName());
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

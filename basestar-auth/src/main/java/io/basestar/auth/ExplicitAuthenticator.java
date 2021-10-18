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

import com.google.common.collect.ImmutableMap;
import io.basestar.util.Name;
import io.swagger.v3.oas.models.security.SecurityScheme;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Warning: this should only be used in a trusted context, e.g. where authentication is provided by an API Gateway
 * and the target is only accessible via that gateway.
 * <p>
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
            public Name getSchema() {

                return Name.of("User");
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

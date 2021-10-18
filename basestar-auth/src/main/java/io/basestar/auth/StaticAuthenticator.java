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
    public boolean canAuthenticate(final Authorization authorization) {

        return true;
    }

    @Override
    public CompletableFuture<Caller> authenticate(final Authorization authorization) {

        return CompletableFuture.completedFuture(caller);
    }

    @Override
    public Map<String, SecurityScheme> openApi() {

        return Collections.emptyMap();
    }
}

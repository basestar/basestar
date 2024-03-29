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
import lombok.Data;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

public interface Authenticator {

    boolean canAuthenticate(Authorization auth);

    CompletableFuture<Caller> authenticate(Authorization auth);

    Map<String, SecurityScheme> openApi();

    @Data
    class Delegating implements Authenticator {

        private final Authenticator delegate;

        @Override
        public boolean canAuthenticate(final Authorization auth) {

            return delegate.canAuthenticate(auth);
        }

        @Override
        public CompletableFuture<Caller> authenticate(final Authorization auth) {

            return delegate.authenticate(auth);
        }

        @Override
        public Map<String, SecurityScheme> openApi() {

            return delegate.openApi();
        }
    }
}

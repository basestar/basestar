package io.basestar.api;

/*-
 * #%L
 * basestar-api
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

import io.basestar.auth.Authenticator;
import io.basestar.auth.Authorization;
import io.basestar.auth.Caller;
import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.security.SecurityScheme;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@Slf4j
public class AuthenticatingAPI implements API {

    private final Authenticator authenticator;

    private final API api;

    public AuthenticatingAPI(final Authenticator authenticator, final API api) {

        this.authenticator = authenticator;
        this.api = api;
    }

    @Override
    public CompletableFuture<APIResponse> handle(final APIRequest request) throws IOException {

        final Authorization authorization = Authorization.from(request.getFirstHeader("Authorization"));
        if(authenticator.canAuthenticate(authorization)) {
            return authenticator.authenticate(authorization).thenCompose(caller -> {

                log.info("Authenticated as {} (anon: {}, super: {})", caller.getId(), caller.isAnon(), caller.isSuper());

                return api.handleUnchecked(new APIRequest.Delegating(request) {

                    @Override
                    public Caller getCaller() {

                        return caller;
                    }
                });
            });
        } else {
            // Leave as anonymous
            return api.handle(request);
        }
    }

    @Override
    public CompletableFuture<OpenAPI> openApi() {

        return api.openApi().thenApply(api -> {
            final Map<String, SecurityScheme> schemes = authenticator.openApi();
            final OpenAPI merge = new OpenAPI();
            merge.setComponents(new Components().securitySchemes(schemes));
            return OpenAPIUtils.merge(api, merge);
        });
    }
}

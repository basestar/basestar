package io.basestar.api;

import io.basestar.auth.Authenticator;
import io.basestar.auth.Caller;
import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.security.SecurityScheme;
import lombok.extern.slf4j.Slf4j;

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
    public CompletableFuture<APIResponse> handle(final APIRequest request) {

        final String authorization = request.getFirstHeader("Authorization");
        return authenticator.authenticate(authorization).thenCompose(caller -> {

            log.info("Authenticated as {} (anon: {}, super: {})", caller.getId(), caller.isAnon(), caller.isSuper());

            return api.handle(new APIRequest.Delegating(request) {

                @Override
                public Caller getCaller() {

                    return caller;
                }
            });
        });
    }

    @Override
    public OpenAPI openApi() {

        final Map<String, SecurityScheme> schemes = authenticator.openApi();
        final OpenAPI merge = new OpenAPI();
        merge.setComponents(new Components().securitySchemes(schemes));
        return OpenAPIUtils.merge(api.openApi(), merge);
    }
}

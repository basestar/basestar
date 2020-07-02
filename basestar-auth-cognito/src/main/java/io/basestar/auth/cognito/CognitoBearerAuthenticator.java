package io.basestar.auth.cognito;

/*-
 * #%L
 * basestar-auth-cognito
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
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jwt.JWTClaimsSet;
import io.basestar.auth.nimbus.NimbusAuthenticator;
import io.basestar.util.Nullsafe;
import io.swagger.v3.oas.models.security.OAuthFlow;
import io.swagger.v3.oas.models.security.OAuthFlows;
import io.swagger.v3.oas.models.security.Scopes;
import io.swagger.v3.oas.models.security.SecurityScheme;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;

public class CognitoBearerAuthenticator extends NimbusAuthenticator {

    private final String domain;

    @lombok.Builder(builderClassName = "Builder")
    CognitoBearerAuthenticator(final String region, final String userPoolId, final String domain) {

        super(JWSAlgorithm.RS256, jwksUrl(Nullsafe.require(region), Nullsafe.require(userPoolId)));
        this.domain = domain;
    }

    private static URL jwksUrl(final String region, final String userPoolId) {

        try {
            return new URL("https://cognito-idp." + region + ".amazonaws.com/" + userPoolId + "/.well-known/jwks.json");
        } catch (final MalformedURLException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    protected String userId(final JWTClaimsSet claims) {

        // For federated login, claims.subject is not the cognito id
        final Object cognitoUsername = claims.getClaim("cognito:username");
        if(cognitoUsername instanceof String) {
            return (String)cognitoUsername;
        } else {
            final Object username = claims.getClaim("username");
            if(username instanceof String) {
                return (String)username;
            } else {
                return super.userId(claims);
            }
        }
    }

    @Override
    public Map<String, SecurityScheme> openApi() {

        if(domain != null) {
            return ImmutableMap.of("Cognito", new SecurityScheme()
                    .type(SecurityScheme.Type.OAUTH2)
                    .flows(new OAuthFlows()
                            .authorizationCode(new OAuthFlow()
                                    .authorizationUrl(domain + "/oauth2/authorize")
                                    .tokenUrl(domain + "/oauth2/authorize")
                                    .scopes(new Scopes().addString("openId", "openid token")))));
        } else {
            return super.openApi();
        }
    }
}

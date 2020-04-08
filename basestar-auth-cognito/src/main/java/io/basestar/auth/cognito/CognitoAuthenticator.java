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

import com.nimbusds.jose.JWSAlgorithm;
import io.basestar.auth.nimbus.NimbusAuthenticator;

import java.net.MalformedURLException;
import java.net.URL;

public class CognitoAuthenticator extends NimbusAuthenticator {

    @Override
    public String getName() {

        return "Cognito";
    }

    public CognitoAuthenticator(final String region, final String userPoolName) {

        super(JWSAlgorithm.RS256, jwksUrl(region, userPoolName));
    }

    private static URL jwksUrl(final String region, final String userPoolName) {

        try {
            return new URL("https://cognito-idp." + region + ".amazonaws.com/" + userPoolName + "/.well-known/jwks.json");
        } catch (final MalformedURLException e) {
            throw new IllegalStateException(e);
        }
    }

//    @Override
//    public Map<String, Object> openApiScheme() {
//
//        return ImmutableMap.of(
//                "type", "oauth2",
//                "flows", ImmutableMap.of(
//                        "authorizationCode", ImmutableMap.of(
//                                "authorizationUrl", domain + "/oauth2/authorize",
//                                "tokenUrl", domain + "/oauth2/authorize",
//                                "scopes", ImmutableMap.of(
//                                        "openid", "openid token"
//                                )
//                        )
//                )
//        );
//
//    }
}

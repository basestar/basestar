package io.basestar.auth.cognito;

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

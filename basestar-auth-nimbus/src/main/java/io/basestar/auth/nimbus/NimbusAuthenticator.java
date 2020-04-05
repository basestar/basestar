package io.basestar.auth.nimbus;

import com.google.common.collect.ImmutableMap;
import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.jwk.source.JWKSource;
import com.nimbusds.jose.jwk.source.RemoteJWKSet;
import com.nimbusds.jose.proc.BadJOSEException;
import com.nimbusds.jose.proc.JWSKeySelector;
import com.nimbusds.jose.proc.JWSVerificationKeySelector;
import com.nimbusds.jose.proc.SecurityContext;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.proc.ConfigurableJWTProcessor;
import com.nimbusds.jwt.proc.DefaultJWTProcessor;
import io.basestar.auth.Authenticator;
import io.basestar.auth.Caller;
import io.basestar.auth.exception.AuthenticationFailedException;
import lombok.Data;

import java.net.URL;
import java.text.ParseException;
import java.util.Map;

@Data
public class NimbusAuthenticator implements Authenticator {

    private final JWSAlgorithm algorithm;

    private final URL jwkURL;

    @Override
    public String getName() {

        return "Bearer";
    }

    @Override
    public Caller authenticate(final String authorization) {

        try {

            if (authorization != null && authorization.startsWith("Bearer ")) {

                final String token = authorization.substring(7).trim();

                final JWKSource<SecurityContext> keySource = new RemoteJWKSet<>(jwkURL);
                final ConfigurableJWTProcessor<SecurityContext> jwtProcessor = new DefaultJWTProcessor<>();
                final JWSKeySelector<SecurityContext> keySelector = new JWSVerificationKeySelector<>(algorithm, keySource);
                jwtProcessor.setJWSKeySelector(keySelector);
                final JWTClaimsSet claims = jwtProcessor.process(token, null);

                return new Caller() {

                    @Override
                    public boolean isAnon() {

                        return false;
                    }

                    @Override
                    public boolean isSuper() {

                        return false;
                    }

                    @Override
                    public String getSchema() {

                        return "User";
                    }

                    @Override
                    public String getId() {

                        return claims.getSubject();
                    }

                    @Override
                    public Map<String, Object> getClaims() {

                        return claims.getClaims();
                    }
                };

            } else {

                return anon();
            }

        } catch (final JOSEException | ParseException | BadJOSEException e) {

            throw new AuthenticationFailedException(e.getMessage(), e);
        }
    }

    @Override
    public Map<String, Object> openApiScheme() {

        return ImmutableMap.of(
                "AccessToken", ImmutableMap.of(
                        "type", "http",
                        "scheme", "bearer",
                        "bearerFormat", "JWT"
                )
        );
    }
}

package io.basestar.auth.nimbus;

/*-
 * #%L
 * basestar-auth-nimbus
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
import io.basestar.auth.Authorization;
import io.basestar.auth.Caller;
import io.basestar.auth.exception.AuthenticationFailedException;
import io.basestar.util.Name;
import io.basestar.util.Nullsafe;
import io.swagger.v3.oas.models.security.SecurityScheme;
import lombok.Data;

import java.net.URL;
import java.text.ParseException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@Data
public class NimbusAuthenticator implements Authenticator {

    private final JWSAlgorithm algorithm;

    private final URL jwkURL;

    @Override
    public boolean canAuthenticate(final Authorization auth) {

        return auth.isBearer();
    }

    @Override
    public CompletableFuture<Caller> authenticate(final Authorization authorization) {

        try {

            final String token = Nullsafe.require(authorization).getCredentials();

            // FIXME: make async
            final JWKSource<SecurityContext> keySource = new RemoteJWKSet<>(jwkURL);
            final ConfigurableJWTProcessor<SecurityContext> jwtProcessor = new DefaultJWTProcessor<>();
            final JWSKeySelector<SecurityContext> keySelector = new JWSVerificationKeySelector<>(algorithm, keySource);
            jwtProcessor.setJWSKeySelector(keySelector);
            final JWTClaimsSet claims = jwtProcessor.process(token, null);

            return CompletableFuture.completedFuture(new Caller() {

                @Override
                public boolean isAnon() {

                    return false;
                }

                @Override
                public boolean isSuper() {

                    return false;
                }

                @Override
                public Name getSchema() {

                    return Name.of("User");
                }

                @Override
                public String getId() {

                    return userId(claims);
                }

                @Override
                public Map<String, Object> getClaims() {

                    return claims.getClaims();
                }
            });

        } catch (final JOSEException | ParseException | BadJOSEException e) {

            throw new AuthenticationFailedException(e.getMessage(), e);
        }
    }

    protected String userId(final JWTClaimsSet claims) {

        return claims.getSubject();
    }

    @Override
    public Map<String, SecurityScheme> openApi() {

        return ImmutableMap.of("Bearer", new SecurityScheme()
                .type(SecurityScheme.Type.HTTP)
                .in(SecurityScheme.In.HEADER)
                .scheme("bearer")
                .bearerFormat("JWT"));
    }
}

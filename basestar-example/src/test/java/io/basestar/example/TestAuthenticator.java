package io.basestar.example;

import io.basestar.auth.Authenticator;
import io.basestar.auth.Caller;

import java.util.Collections;
import java.util.Map;

public class TestAuthenticator implements Authenticator {

    @Override
    public String getName() {

        return "test";
    }

    @Override
    public Caller authenticate(final String authorization) {

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

                return authorization;
            }

            @Override
            public Map<String, Object> getClaims() {

                return Collections.emptyMap();
            }
        };
    }

    @Override
    public Map<String, Object> openApiScheme() {

        return null;
    }

}

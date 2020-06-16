package io.basestar.auth;

import io.basestar.auth.exception.AuthenticationFailedException;

public interface Authorization {

    String getType();

    String getCredentials();

    static Authorization of(final String type, final String credentials) {

        return new Authorization() {
            @Override
            public String getType() {
                return type;
            }

            @Override
            public String getCredentials() {
                return credentials;
            }
        };
    }

    static Authorization from(final String str) {

        if(str == null) {
            return of(null, null);
        } else {
            final String[] parts = str.split(" ", 2);
            if (parts.length == 2) {
                return of(parts[0].trim(), parts[1].trim());
            } else {
                throw new AuthenticationFailedException("Authorization did not match required format");
            }
        }
    }

    default boolean isAnon() {

        return getType() == null;
    }

    default boolean isBasic() {

        return "Basic".equalsIgnoreCase(getType());
    }

    default boolean isBearer() {

        return "Bearer".equalsIgnoreCase(getType());
    }
}

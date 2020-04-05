package io.basestar.auth;

import java.util.Map;

public interface Authenticator {

    String getName();

    Caller authenticate(String authorization);

    Map<String, Object> openApiScheme();

    default Caller anon() {

        return Caller.ANON;
    }

    default Caller superuser() {

        return Caller.SUPER;
    }
}
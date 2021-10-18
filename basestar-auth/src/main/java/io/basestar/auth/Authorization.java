package io.basestar.auth;

/*-
 * #%L
 * basestar-auth
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

        if (str == null || str.isEmpty()) {
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

        return is("Basic");
    }

    default boolean isBearer() {

        return is("Bearer");
    }

    default boolean is(final String type) {

        return type.equalsIgnoreCase(getType());
    }
}

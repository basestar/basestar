package io.basestar.example;

/*-
 * #%L
 * basestar-example
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

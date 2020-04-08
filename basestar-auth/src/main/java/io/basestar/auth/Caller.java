package io.basestar.auth;

/*-
 * #%L
 * basestar-auth
 * $Id:$
 * $HeadURL:$
 * %%
 * Copyright (C) 2020 basestar.io
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

import lombok.Data;

import java.util.Collections;
import java.util.Map;

public interface Caller {

    Anon ANON = new Anon();

    Super SUPER = new Super();

    boolean isAnon();

    boolean isSuper();

    String getSchema();

    String getId();

    Map<String, Object> getClaims();

    @Data
    class Delegating implements Caller {

        private final Caller delegate;

        @Override
        public boolean isAnon() {

            return delegate.isAnon();
        }

        @Override
        public boolean isSuper() {

            return delegate.isSuper();
        }

        @Override
        public String getSchema() {

            return delegate.getSchema();
        }

        @Override
        public String getId() {

            return delegate.getId();
        }

        @Override
        public Map<String, Object> getClaims() {

            return delegate.getClaims();
        }
    }

    class Anon implements Caller {

        @Override
        public boolean isAnon() {

            return true;
        }

        @Override
        public boolean isSuper() {

            return false;
        }

        @Override
        public String getSchema() {

            return null;
        }

        @Override
        public String getId() {

            return null;
        }

        @Override
        public Map<String, Object> getClaims() {

            return Collections.emptyMap();
        }
    }

    class Super implements Caller {

        @Override
        public boolean isAnon() {

            return false;
        }

        @Override
        public boolean isSuper() {

            return true;
        }

        @Override
        public String getSchema() {

            return null;
        }

        @Override
        public String getId() {

            return null;
        }

        @Override
        public Map<String, Object> getClaims() {

            return Collections.emptyMap();
        }
    }

    //boolean verify(Permission permission, Map<String, Object> context);
}

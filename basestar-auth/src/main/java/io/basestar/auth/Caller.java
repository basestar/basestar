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

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import io.basestar.util.Name;
import lombok.Data;

import java.util.Collections;
import java.util.Map;

@JsonDeserialize(as = SimpleCaller.class)
public interface Caller {

    Anon ANON = new Anon();

    Super SUPER = new Super();

    boolean isAnon();

    boolean isSuper();

    @JsonSerialize(using = ToStringSerializer.class)
    Name getSchema();

    String getId();

    Map<String, Object> getClaims();

    static SimpleCaller.Builder builder() {

        return new SimpleCaller.Builder();
    }

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
        public Name getSchema() {

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
        public Name getSchema() {

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
        public Name getSchema() {

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
}

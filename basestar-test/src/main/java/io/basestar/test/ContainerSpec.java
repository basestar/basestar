package io.basestar.test;

/*-
 * #%L
 * basestar-test
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

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import lombok.Data;
import lombok.experimental.Accessors;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

@Data
public class ContainerSpec {

    private final String image;

    private final List<String> env;

    private final List<PortSpec> ports;

    private final Pattern waitFor;

    private final String hash;

    private ContainerSpec(final Builder builder) {

        this.image = builder.image();
        this.env = ImmutableList.copyOf(builder.env());
        this.ports = ImmutableList.copyOf(builder.ports());
        this.waitFor = builder.waitFor();
        @SuppressWarnings("all")
        final Hasher hasher = Hashing.md5().newHasher();
        hasher.putString(image, Charsets.UTF_8);
        env.stream().sorted().forEach(e ->
                hasher.putString(e, Charsets.UTF_8));
        ports.stream().map(PortSpec::toString).sorted().forEach(p ->
                hasher.putString(p, Charsets.UTF_8));
        this.hash = hasher.hash().toString();
    }

    public static Builder builder() {

        return new Builder();
    }

    @Override
    public String toString() {

        return image + " HASH: " + hash + ", ENV: " + env + ", PORTS: " + ports;
    }

    @Data
    @Accessors(chain = true)
    public static class PortSpec {

        private final int host;

        private final int container;

        public static PortSpec of(final int both) {

            return of(both, both);
        }

        public static PortSpec of(final int host, final int container) {

            return new PortSpec(host, container);
        }

        @Override
        public String toString() {

            return host + ":" + container;
        }
    }

    @Data
    @Accessors(chain = true, fluent = true)
    public static class Builder {

        private String image;

        private final List<String> env = new ArrayList<>();

        private final List<PortSpec> ports = new ArrayList<>();

        private Pattern waitFor;

        public ContainerSpec build() {

            return new ContainerSpec(this);
        }

        public Builder env(final String env) {

            this.env.add(env);
            return this;
        }

        public Builder port(final PortSpec port) {

            this.ports.add(port);
            return this;
        }

        public Builder port(final int both) {

            this.ports.add(PortSpec.of(both));
            return this;
        }

        public Builder port(final int host, final int container) {

            this.ports.add(PortSpec.of(host, container));
            return this;
        }
    }
}

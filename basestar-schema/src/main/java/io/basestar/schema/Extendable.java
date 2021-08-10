package io.basestar.schema;

/*-
 * #%L
 * basestar-schema
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

import com.fasterxml.jackson.annotation.JsonInclude;

import java.io.Serializable;
import java.util.Map;
import java.util.Optional;

public interface Extendable {

    interface Builder<B extends Builder<B>> {

        B setExtensions(Map<String, Serializable> extensions);
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    Map<String, Serializable> getExtensions();

    @SuppressWarnings("unchecked")
    default <T extends Serializable> T getExtension(final String name) {

        return (T) this.getExtension(Serializable.class, name);
    }

    default <T extends Serializable> T getExtension(final Class<T> of, final String name) {

        return this.getOptionalExtension(of, name).orElse(null);
    }

    default <T extends Serializable> Optional<T> getOptionalExtension(final Class<T> of, final String name) {

        final Map<String, Serializable> extensions = getExtensions();
        if (extensions != null) {
            if (extensions.containsKey(name)) {
                return Optional.of(of.cast(extensions.get(name)));
            }
        }
        return Optional.empty();
    }
}

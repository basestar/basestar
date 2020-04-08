package io.basestar.schema.use;

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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import io.basestar.schema.Expander;
import io.basestar.schema.Instance;
import io.basestar.schema.Schema;
import io.basestar.util.Path;

import java.util.Collections;
import java.util.Set;

public interface UseScalar<T> extends Use<T> {

    @Override
    default Use<T> resolve(final Schema.Resolver resolver) {

        return this;
    }

    @Override
    default T expand(final T value, final Expander expander, final Set<Path> expand) {

        return value;
    }

    @Override
    @Deprecated
    default Set<Path> requireExpand(final Set<Path> paths) {

        return Collections.emptySet();
    }

    @Override
    @Deprecated
    default Multimap<Path, Instance> refs(final T value) {

        return HashMultimap.create();
    }

    @Override
    default Use<?> typeOf(final Path path) {

        if(path.isEmpty()) {
            return this;
        } else {
            throw new IllegalStateException();
        }
    }
}

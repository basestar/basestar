package io.basestar.schema;

/*-
 * #%L
 * basestar-schema
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

import io.basestar.util.PagedList;
import io.basestar.util.Path;

import java.util.Set;

public interface Expander {

    static Expander noop() {

        return new Expander() {
            @Override
            public Instance ref(final ObjectSchema schema, final Instance ref, final Set<Path> expand) {

                return ref;
            }

            @Override
            public PagedList<Instance> link(final Link link, final PagedList<Instance> value, final Set<Path> expand) {

                return value;
            }
        };
    }

    Instance ref(ObjectSchema schema, Instance ref, Set<Path> expand);

    PagedList<Instance> link(Link link, PagedList<Instance> value, Set<Path> expand);
}

package io.basestar.schema.util;

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

import io.basestar.schema.Instance;
import io.basestar.schema.InstanceSchema;
import io.basestar.schema.Link;
import io.basestar.schema.ObjectSchema;
import io.basestar.util.Name;
import io.basestar.util.Page;

import java.util.Set;

public interface Expander {

    static Expander noop() {

        return new Expander() {
            @Override
            public Instance expandRef(final ObjectSchema schema, final Instance ref, final Set<Name> expand) {

                if(ref == null) {
                    return null;
                } else {
                    return schema.expand(ref, this, expand);
                }
            }

            @Override
            public Page<Instance> expandLink(final Link link, final Page<Instance> value, final Set<Name> expand) {

                if(value == null) {
                    return null;
                } else {
                    final InstanceSchema schema = link.getSchema();
                    return value.map(v -> v == null ? null : schema.expand(v, this, expand));
                }
            }
        };
    }

    Instance expandRef(ObjectSchema schema, Instance ref, Set<Name> expand);

    Page<Instance> expandLink(Link link, Page<Instance> value, Set<Name> expand);
}

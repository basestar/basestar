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

import io.basestar.expression.Context;
import io.basestar.schema.Instance;
import io.basestar.schema.InstanceSchema;
import io.basestar.schema.Schema;
import io.basestar.util.Name;

import java.util.Set;

public interface UseInstance extends UseNamed<Instance> {

    InstanceSchema getSchema();

    @Override
    default Name getQualifiedName() {

        return getSchema().getQualifiedName();
    }

    @Override
    default UseInstance resolve(final Schema.Resolver resolver) {

        return this;
    }

    @Override
    default Use<?> typeOf(final Name name) {

        if(name.isEmpty()) {
            return this;
        } else {
            return getSchema().typeOf(name);
        }
    }

    @Override
    default Instance applyVisibility(final Context context, final Instance value) {

        if(value == null) {
            return null;
        } else {
            return getSchema().applyVisibility(context, value);
        }
    }

    @Override
    default Instance evaluateTransients(final Context context, final Instance value, final Set<Name> expand) {

        if(value == null) {
            return null;
        } else {
            return getSchema().evaluateTransients(context, value, expand);
        }
    }

    @Override
    default Set<Name> transientExpand(final Name name, final Set<Name> expand) {

        return getSchema().transientExpand(name, expand);
    }
}

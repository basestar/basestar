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

import java.lang.reflect.Type;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public interface UseInstance extends UseNamed<Instance>, UseLayout {

    InstanceSchema getSchema();

    default Name getName() {

        return getSchema().getQualifiedName();
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
    default Type javaType(final Name name) {

        if(name.isEmpty()) {
            return Object.class;
        } else {
            return getSchema().javaType(name);
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

    @Override
    default void collectDependencies(final Set<Name> expand, final Map<Name, Schema<?>> out) {

        final Schema<?> schema = getSchema();
        // FIXME:?
        if(expand == null || expand.isEmpty()) {
            out.put(schema.getQualifiedName(), schema);
        } else {
            schema.collectDependencies(expand, out);
        }
    }

    @Override
    default String toString(final Instance value) {

        return Objects.toString(value);
    }

    @Override
    default boolean equal(final Instance a, final Instance b) {

        return getSchema().equal(a, b);
    }
}

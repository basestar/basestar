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
import io.basestar.schema.*;
import io.basestar.util.Name;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public interface UseInstance extends UseNamed<Instance> {

    InstanceSchema getSchema();

    default Name getName() {

        return getSchema().getQualifiedName();
    }

    static UseInstance from(final InstanceSchema schema, final Object config) {

        if(schema instanceof LinkableSchema) {
            return UseLinkable.from((LinkableSchema)schema, config);
        } else if(schema instanceof StructSchema) {
            return UseStruct.from((StructSchema)schema, config);
        } else {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    default Optional<Use<?>> optionalTypeOf(final Name name) {

        if(name.isEmpty()) {
            return Optional.of(this);
        } else {
            return (Optional<Use<?>>)(Optional<?>)getSchema().optionalTypeOf(name);
        }
    }

    @Override
    default Type javaType(final Name name) {

        return getSchema().javaType(name);
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
    default void collectDependencies(final Set<Name> expand, final Map<Name, Schema> out) {

        final Schema schema = getSchema();
        if (expand == null || expand.isEmpty()) {
            out.put(schema.getQualifiedName(), schema);
        } else {
            schema.collectDependencies(expand, out);
        }
    }

    @Override
    default void collectMaterializationDependencies(final Set<Name> expand, final Map<Name, Schema> out) {

        final Schema schema = getSchema();
        schema.collectMaterializationDependencies(expand, out);
    }

    @Override
    default String toString(final Instance value) {

        return getSchema().toString(value);
    }

    @Override
    default boolean areEqual(final Instance a, final Instance b) {

        return getSchema().areEqual(a, b);
    }

    @Override
    default boolean isCompatibleBucketing(final List<Bucketing> other, final Name name) {

        return getSchema().isCompatibleBucketing(other, name);
    }
}

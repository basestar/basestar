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
import io.basestar.expression.Expression;
import io.basestar.schema.Bucketing;
import io.basestar.schema.Constraint;
import io.basestar.schema.LinkableSchema;
import io.basestar.schema.Schema;
import io.basestar.schema.util.Bucket;
import io.basestar.schema.util.Expander;
import io.basestar.schema.util.Ref;
import io.basestar.util.Name;

import java.util.*;

public interface UseScalar<T> extends Use<T> {

    @Override
    default Set<Constraint.Violation> validate(final Context context, final Name name, final T value) {

        return Collections.emptySet();
    }

    @Override
    default UseScalar<T> resolve(final Schema.Resolver resolver) {

        return this;
    }

    @Override
    default T expand(final Name parent, final T value, final Expander expander, final Set<Name> expand) {

        return value;
    }

    @Override
    default void expand(final Name parent, final Expander expander, final Set<Name> expand) {

    }

    @Override
    default T applyVisibility(final Context context, final T value) {

        return value;
    }

    @Override
    default Set<Expression> refQueries(final Name otherSchemaName, final Set<Name> expand, final Name name) {

        return Collections.emptySet();
    }

    @Override
    default Set<Name> refExpand(final Name otherSchemaName, final Set<Name> expand) {

        return Collections.emptySet();
    }

    @Override
    default Map<Ref, Long> refVersions(final T value) {

        return Collections.emptyMap();
    }

    @Override
    default T evaluateTransients(final Context context, final T value, final Set<Name> expand) {

        return value;
    }

    @Override
    @Deprecated
    default Set<Name> requiredExpand(final Set<Name> names) {

        return Collections.emptySet();
    }

    @Override
    default boolean isCompatibleBucketing(final List<Bucketing> other, final Name name) {

        return false;
    }

    @Override
    default Use<?> typeOf(final Name name) {

        if(name.isEmpty()) {
            return this;
        } else {
            throw new IllegalStateException();
        }
    }

    @Override
    default Set<Name> transientExpand(final Name name, final Set<Name> expand) {

        return Collections.emptySet();
    }

    @Override
    default void collectDependencies(final Set<Name> expand, final Map<Name, Schema<?>> out) {

    }

    @Override
    default void collectMaterializationDependencies(final Set<Name> expand, final Map<Name, LinkableSchema> out) {

    }

    @Override
    default String toString(final T value) {

        return Objects.toString(value);
    }

    @Override
    default Object[] key(final T value) {

        return new Object[]{value};
    }
}

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
import io.basestar.expression.constant.NameConstant;
import io.basestar.expression.iterate.ContextIterator;
import io.basestar.expression.iterate.ForAny;
import io.basestar.schema.Bucketing;
import io.basestar.schema.Constraint;
import io.basestar.schema.Schema;
import io.basestar.schema.util.Cascade;
import io.basestar.schema.util.Expander;
import io.basestar.schema.util.Ref;
import io.basestar.util.Name;

import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

public interface UseCollection<V, T extends Collection<V>> extends UseContainer<V, T> {

    T transformValues(T value, BiFunction<Use<V>, V, V> fn);

    @Override
    <V2> UseCollection<V2, ? extends Collection<V2>> transform(Function<Use<V>, Use<V2>> fn);

    @Override
    default Set<Constraint.Violation> validateType(final Context context, final Name name, final T value) {
        final Use<V> type = getType();
        return value.stream().flatMap(v -> type.validate(context, name, v).stream()).collect(Collectors.toSet());
    }

    @Override
    default void serializeValue(final T value, final DataOutput out) throws IOException {

        final Use<V> type = getType();
        out.writeInt(value.size());
        for (final V v : value) {
            type.serialize(v, out);
        }
    }

    @Override
    default Set<Expression> refQueries(final Name otherSchemaName, final Set<Name> expand, final Name name) {

        final int hash = System.identityHashCode(this);
        final String v = "v" + hash;
        return getType().refQueries(otherSchemaName, expand, Name.of(v)).stream().map(
                q -> new ForAny(q, new ContextIterator.OfValue(v, new NameConstant(name)))
        ).collect(Collectors.toSet());
    }

    @Override
    default Set<Expression> cascadeQueries(final Cascade cascade, final Name otherSchemaName, final Name name) {

        final int hash = System.identityHashCode(this);
        final String v = "v" + hash;
        return getType().cascadeQueries(cascade, otherSchemaName, Name.of(v)).stream().map(
                q -> new ForAny(q, new ContextIterator.OfValue(v, new NameConstant(name)))
        ).collect(Collectors.toSet());
    }

    @Override
    default Set<Name> refExpand(final Name otherSchemaName, final Set<Name> expand) {

        return getType().refExpand(otherSchemaName, expand);
    }

    @Override
    default Map<Ref, Long> refVersions(final T value) {

        if (value == null) {
            return Collections.emptyMap();
        }
        final Map<Ref, Long> versions = new HashMap<>();
        value.forEach(v -> versions.putAll(getType().refVersions(v)));
        return versions;
    }

    @Override
    default Optional<Use<?>> optionalTypeOf(final Name name) {

        if (name.isEmpty()) {
            return Optional.of(this);
        } else {
            return getType().optionalTypeOf(name);
        }
    }

    @Override
    default T expand(final Name parent, final T value, final Expander expander, final Set<Name> expand) {

        final Use<V> type = getType();
        return transformValues(value, before -> type.expand(parent, before, expander, expand));
    }

    @Override
    default void expand(final Name parent, final Expander expander, final Set<Name> expand) {

        final Use<V> type = getType();
        type.expand(parent, expander, expand);
    }

    @Override
    default T applyVisibility(final Context context, final T value) {

        final Use<V> type = getType();
        return transformValues(value, before -> type.applyVisibility(context, before));
    }

    @Override
    default T evaluateTransients(final Context context, final T value, final Set<Name> expand) {

        final Use<V> type = getType();
        return transformValues(value, before -> type.evaluateTransients(context, before, expand));
    }

    @Override
    default Set<Name> transientExpand(final Name name, final Set<Name> expand) {

        return getType().transientExpand(name, expand);
    }

    @Override
    default Set<Name> requiredExpand(final Set<Name> names) {

        return getType().requiredExpand(names);
    }

    @Override
    default void collectDependencies(final Set<Name> expand, final Map<Name, Schema> out) {

        getType().collectDependencies(expand, out);
    }

    @Override
    default void collectMaterializationDependencies(final Set<Name> expand, final Map<Name, Schema> out) {

        getType().collectMaterializationDependencies(expand, out);
    }

    @Override
    default boolean isCompatibleBucketing(final List<Bucketing> other, final Name name) {

        return getType().isCompatibleBucketing(other, name);
    }

    @Override
    default String toString(final T value) {

        if (value == null) {
            return "null";
        } else {
            final Use<V> type = getType();
            return "[" + value.stream().map(type::toString).collect(Collectors.joining(", ")) + "]";
        }
    }
}


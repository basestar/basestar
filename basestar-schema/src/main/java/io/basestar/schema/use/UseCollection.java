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
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.expression.constant.NameConstant;
import io.basestar.expression.iterate.ForAny;
import io.basestar.expression.iterate.Of;
import io.basestar.schema.Constraint;
import io.basestar.schema.Instance;
import io.basestar.schema.util.Expander;
import io.basestar.schema.util.Ref;
import io.basestar.util.Name;

import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public interface UseCollection<V, T extends Collection<V>> extends Use<T> {

    Use<V> getType();

    T transform(T value, Function<V, V> fn);

    @Override
    default Set<Constraint.Violation> validate(final Context context, final Name name, final T value) {

        if(value == null) {
            return Collections.emptySet();
        } else {
            final Use<V> type = getType();
            return value.stream().flatMap(v -> type.validate(context, name, v).stream()).collect(Collectors.toSet());
        }
    }

    @Override
    default void serializeValue(final T value, final DataOutput out) throws IOException {

        final Use<V> type = getType();
        out.writeInt(value.size());
        for(final V v : value) {
            type.serialize(v, out);
        }
    }

    @Override
    default Set<Expression> refQueries(final Name otherSchemaName, final Set<Name> expand, final Name name) {

        final int hash = System.identityHashCode(this);
        final String v = "v" + hash;
        return getType().refQueries(otherSchemaName, expand, Name.of(v)).stream().map(
                q -> new ForAny(q, new Of(v, new NameConstant(name)))
        ).collect(Collectors.toSet());
    }

    @Override
    default Set<Name> refExpand(final Name otherSchemaName, final Set<Name> expand) {

        return getType().refExpand(otherSchemaName, expand);
    }

    @Override
    default Map<Ref, Long> refVersions(final T value) {

        if(value == null) {
            return Collections.emptyMap();
        }
        final Map<Ref, Long> versions = new HashMap<>();
        value.forEach(v -> versions.putAll(getType().refVersions(v)));
        return versions;
    }

    @Override
    default Use<?> typeOf(final Name name) {

        if(name.isEmpty()) {
            return this;
        } else {
            return getType().typeOf(name);
        }
    }

    @Override
    default T expand(final T value, final Expander expander, final Set<Name> expand) {

        final Use<V> type = getType();
        return transform(value, before -> type.expand(before, expander, expand));
    }

    @Override
    default T applyVisibility(final Context context, final T value) {

        final Use<V> type = getType();
        return transform(value, before -> type.applyVisibility(context, before));
    }

    @Override
    default T evaluateTransients(final Context context, final T value, final Set<Name> expand) {

        final Use<V> type = getType();
        return transform(value, before -> type.evaluateTransients(context, before, expand));
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
    @Deprecated
    default Multimap<Name, Instance> refs(final T value) {

        final Multimap<Name, Instance> result = HashMultimap.create();
        if(value != null) {
            value.forEach(v -> getType().refs(v).forEach(result::put));
        }
        return result;
    }
}


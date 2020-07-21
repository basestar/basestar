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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.expression.constant.NameConstant;
import io.basestar.expression.iterate.ForAny;
import io.basestar.expression.iterate.Of;
import io.basestar.schema.Constraint;
import io.basestar.schema.Instance;
import io.basestar.schema.Schema;
import io.basestar.schema.exception.InvalidTypeException;
import io.basestar.schema.util.Expander;
import io.basestar.schema.util.Ref;
import io.basestar.util.Name;
import io.swagger.v3.oas.models.media.MapSchema;
import lombok.Data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Map Type
 *
 * <strong>Example</strong>
 * <pre>
 * type:
 *   map: string
 * </pre>
 */

@Data
public class UseMap<T> implements Use<Map<String, T>> {

    public static final String NAME = "map";

    public static final String EXPAND_WILDCARD = "*";

    private final Use<T> type;

    @Override
    public <R> R visit(final Visitor<R> visitor) {

        return visitor.visitMap(this);
    }

    public static UseMap<?> from(final Object config) {

        return Use.fromNestedConfig(config, (type, nestedConfig) -> new UseMap<>(type));
    }

    @Override
    public Object toConfig() {

        return ImmutableMap.of(
                NAME, type
        );
    }

    @Override
    public UseMap<?> resolve(final Schema.Resolver resolver) {

        final Use<?> resolved = type.resolve(resolver);
        if(resolved == type) {
            return this;
        } else {
            return new UseMap<>(resolved);
        }
    }

    @Override
    public Map<String, T> create(final Object value, final boolean expand, final boolean suppress) {

        return create(value, suppress, v -> type.create(v, expand, suppress));
    }

    public static <T> Map<String, T> create(final Object value, final boolean suppress, final Function<Object, T> fn) {

        if(value == null) {
            return null;
        } else if(value instanceof Map) {
            return ((Map<?, ?>) value).entrySet().stream()
                    .collect(Collectors.toMap(
                            entry -> entry.getKey().toString(),
                            entry -> fn.apply(entry.getValue())
                    ));
        } else if(suppress) {
            return null;
        } else {
            throw new InvalidTypeException();
        }
    }

    @Override
    public Code code() {

        return Code.MAP;
    }

    @Override
    public io.swagger.v3.oas.models.media.Schema<?> openApi() {

        return new MapSchema().additionalProperties(type.openApi());
    }

    @Override
    public void serializeValue(final Map<String, T> value, final DataOutput out) throws IOException {

        out.writeInt(value.size());
        for(final Map.Entry<String, T> entry : new TreeMap<>(value).entrySet()) {
            UseString.DEFAULT.serializeValue(entry.getKey(), out);
            type.serialize(entry.getValue(), out);
        }
    }

    @Override
    public Map<String, T> deserializeValue(final DataInput in) throws IOException {

        return deserializeAnyValue(in);
    }

    public static <T> Map<String, T> deserializeAnyValue(final DataInput in) throws IOException {

        final Map<String, T> result = new HashMap<>();
        final int size = in.readInt();
        for(int i = 0; i != size; ++i) {
            final String key = UseString.DEFAULT.deserializeValue(in);
            final T value = Use.deserializeAny(in);
            result.put(key, value);
        }
        return result;
    }

    @Override
    public Set<Expression> refQueries(final Name otherSchemaName, final Set<Name> expand, final Name name) {

        final int hash = System.identityHashCode(this);
        final String k = "k" + hash;
        final String v = "v" + hash;
        // FIXME: expand
        return getType().refQueries(otherSchemaName, expand, Name.of(v)).stream().map(
                q -> new ForAny(q, new Of(k, v, new NameConstant(name)))
        ).collect(Collectors.toSet());
    }

    @Override
    public Set<Name> refExpand(final Name otherSchemaName, final Set<Name> expand) {

        // FIXME: expand
        return getType().refExpand(otherSchemaName, expand);
    }

    @Override
    public Map<Ref, Long> refVersions(final Map<String, T> value) {

        if(value == null) {
            return Collections.emptyMap();
        }
        final Map<Ref, Long> versions = new HashMap<>();
        value.forEach((k, v) -> versions.putAll(getType().refVersions(v)));
        return versions;
    }

    @Override
    public Use<?> typeOf(final Name name) {

        if(name.isEmpty()) {
            return this;
        } else {
            return type.typeOf(name);
        }
    }

//    @Override
//    public void serialize(final Map<String, T> value, final DataOutput out) throws IOException {
//
//        if(value != null) {
//            out.writeInt(Code.MAP.ordinal());
//            for(final Map.Entry<String, T> entry : new TreeMap<>(value).entrySet()) {
//                out.write((entry.getKey()).getBytes(Charsets.UTF_8));
//                type.serialize(entry.getValue(), out);
//            }
//        } else {
//            out.writeInt(Code.NULL.ordinal());
//        }
//    }

//    @Override
//    public Map<String, T> expand(final Map<String, T> value, final Expander expander, final Set<Path> expand) {
//
//        if(value != null) {
//            final Map<String, T> changed = new HashMap<>();
//            final Map<String, Set<Path>> branch = Path.branch(expand);
//            for(final Map.Entry<String, T> entry : value.entrySet()) {
//                Set<Path> branchExpand = branch.get(entry.getKey());
//                if(branchExpand == null) {
//                    branchExpand = branch.get(EXPAND_WILDCARD);
//                }
////                if(branchExpand != null) {
//                final T before = entry.getValue();
//                final T after = type.expand(before, expander, branchExpand);
//                if(before != after) {
//                    changed.put(entry.getKey(), after);
//                }
////                }
//            }
//            if(changed.isEmpty()) {
//                return value;
//            } else {
//                final Map<String, T> copy = new HashMap<>(value);
//                copy.putAll(changed);
//                return copy;
//            }
//        } else {
//            return null;
//        }
//    }

    private static Set<Name> branch(final Map<String, Set<Name>> branches, final String key) {

        final Set<Name> branch = branches.get(key);
        if(branch == null) {
            return branches.get(EXPAND_WILDCARD);
        } else {
            return branch;
        }
    }

    @Override
    public Map<String, T> expand(final Map<String, T> value, final Expander expander, final Set<Name> expand) {

        final Map<String, Set<Name>> branches = Name.branch(expand);
        return transform(value, (key, before) -> {
            final Set<Name> branch = branch(branches, key);
            if(branch != null) {
                return type.expand(before, expander, branch);
            } else {
                return before;
            }
        });
    }

    @Override
    public Map<String, T> applyVisibility(final Context context, final Map<String, T> value) {

        return transform(value, (key, before) -> type.applyVisibility(context, before));
    }

    @Override
    public Map<String, T> evaluateTransients(final Context context, final Map<String, T> value, final Set<Name> expand) {

        final Map<String, Set<Name>> branches = Name.branch(expand);
        return transform(value, (key, before) -> {
            final Set<Name> branch = branch(branches, key);
            if(branch != null) {
                return type.evaluateTransients(context, before, branch);
            } else {
                return before;
            }
        });
    }

    @Override
    public Set<Name> transientExpand(final Name name, final Set<Name> expand) {

        final Map<String, Set<Name>> branch = Name.branch(expand);
        final Set<Name> result = new HashSet<>(expand);
        branch.forEach((k, v) -> result.addAll(type.transientExpand(name.with(k), v)));
        return result;
    }

    @Override
    public Set<Constraint.Violation> validate(final Context context, final Name name, final Map<String, T> value) {

        if(value == null) {
            return Collections.emptySet();
        } else {
            return value.entrySet().stream()
                    .flatMap(e -> type.validate(context, name.with(e.getKey()), e.getValue()).stream())
                    .collect(Collectors.toSet());
        }
    }

    private static <T> Map<String, T> transform(final Map<String, T> value, final BiFunction<String, T, T> fn) {

        if(value != null) {
            final Map<String, T> changed = new HashMap<>();
            for(final Map.Entry<String, T> entry : value.entrySet()) {
                final T before = entry.getValue();
                final T after = fn.apply(entry.getKey(), before);
                if(before != after) {
                    changed.put(entry.getKey(), after);
                }
            }
            if(changed.isEmpty()) {
                return value;
            } else {
                final Map<String, T> copy = new HashMap<>(value);
                copy.putAll(changed);
                return copy;
            }
        } else {
            return null;
        }
    }

    @Override
    public Set<Name> requiredExpand(final Set<Name> names) {

        final Set<Name> result = new HashSet<>();
        Name.branch(names)
                .forEach((head, tail) -> type.requiredExpand(tail)
                        .forEach(path -> result.add(Name.of(head).with(path))));
        return result;
    }

    @Override
    public Multimap<Name, Instance> refs(final Map<String, T> value) {

        final Multimap<Name, Instance> result = HashMultimap.create();
        if(value != null) {
            value.forEach((k, v) -> type.refs(v).forEach((k2, v2) ->
                    result.put(Name.of(k).with(k2), v2)));
        }
        return result;
    }

    @Override
    public String toString() {

        return NAME + "<" + type + ">";
    }

    @Override
    public void collectDependencies(final Set<Name> expand, final Map<Name, Schema<?>> out) {

        final Set<Name> union = Name.branch(expand).values().stream().reduce(Collections.emptySet(), Sets::union);
        type.collectDependencies(union, out);
    }
}

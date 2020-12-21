package io.basestar.schema;

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

import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.schema.exception.UnexpectedTypeException;
import io.basestar.schema.use.Use;
import io.basestar.schema.use.UseInstance;
import io.basestar.schema.use.UseString;
import io.basestar.schema.use.ValueContext;
import io.basestar.schema.util.Expander;
import io.basestar.schema.util.Ref;
import io.basestar.util.Name;
import io.leangen.geantyref.TypeFactory;

import java.io.*;
import java.lang.reflect.Type;
import java.util.*;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

public interface InstanceSchema extends Schema<Instance>, Member.Resolver, Property.Resolver, Layout {

    interface Descriptor<S extends InstanceSchema> extends Schema.Descriptor<S, Instance>, Property.Resolver.Descriptor {

        interface Self<S extends InstanceSchema> extends Schema.Descriptor.Self<S, Instance>, Descriptor<S> {

            @Override
            default Map<String, Property.Descriptor> getProperties() {

                return self().describeDeclaredProperties();
            }
        }
    }

    interface Builder<B extends Builder<B, S>, S extends InstanceSchema> extends Schema.Builder<B, S, Instance>, Descriptor<S>, Property.Resolver.Builder<B> {

    }

    SortedMap<String, Use<?>> metadataSchema();

    Instance create(ValueContext context, Map<String, Object> value, Set<Name> expand);

    @Override
    UseInstance typeOf();

    boolean isConcrete();

    @Override
    Descriptor<? extends InstanceSchema> descriptor();

    @Override
    default Set<Name> getExpand() {

        return Collections.emptySet();
    }

    @Override
    default Map<Name, Schema<?>> dependencies() {

        return dependencies(getExpand());
    }

    @Override
    default Map<String, Use<?>> getSchema() {

        final SortedMap<String, Use<?>> result = new TreeMap<>();
        metadataSchema().forEach(result::put);
        getMembers().forEach((name, member) -> result.put(name, member.typeOf()));
        return result;
    }

    default boolean hasMember(final String name) {

        return metadataSchema().containsKey(name) || getMember(name, true) != null;
    }

    default SortedMap<String, Use<?>> layoutSchema(final Set<Name> expand) {

        // This is the canonical layout, by definition
        final SortedMap<String, Use<?>> result = new TreeMap<>();
        metadataSchema().forEach(result::put);
        final Map<String, Set<Name>> branches = Name.branch(expand);
        getMembers().forEach((name, member) -> {
            final Set<Name> branch = branches.get(name);
            member.layout(branch).ifPresent(memberLayout -> result.put(name, memberLayout));
        });
        return result;
    }

    default boolean hasProperties() {

        return !getProperties().isEmpty();
    }

    default boolean hasMutableProperties() {

        return !getProperties().values().stream().map(Property::isImmutable).reduce(true, (a, b) -> a && b);
    }

    default InstanceSchema resolveExtended(final Name name) {

        if(getQualifiedName().equals(name)) {
            return this;
        } else {
            throw new IllegalStateException(name + " is not a valid subtype of " + getQualifiedName());
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    default Instance create(final ValueContext context, final Object value, final Set<Name> expand) {

        if(value == null) {
            return null;
        } else if(value instanceof Map) {
            return create(context, (Map<String, Object>) value, expand);
        } else {
            throw new UnexpectedTypeException(this, value);
        }
    }

    default Set<Name> requiredExpand(final Set<Name> names) {

        final Set<Name> result = new HashSet<>();
        for (final Map.Entry<String, Set<Name>> branch : Name.branch(names).entrySet()) {
            final Member member = getMember(branch.getKey(), true);
            if(member != null) {
                for(final Name tail : member.requiredExpand(branch.getValue())) {
                    result.add(Name.of(branch.getKey()).with(tail));
                }
            }
        }
        return Name.simplify(result);
    }

    default Set<Name> transientExpand(final Name path, final Set<Name> expand) {

        final Set<Name> transientExpand = new HashSet<>(expand);
        final Map<String, Set<Name>> branch = Name.branch(expand);
        getMembers().forEach((name, member) -> {
            if(branch.containsKey(name)) {
                final Set<Name> memberExpand = branch.get(name);
                transientExpand.addAll(member.transientExpand(path.with(name), memberExpand));
            }
        });
        return transientExpand;
    }

    @SuppressWarnings("unchecked")
    default <T> Use<T> typeOf(final Name name) {

        if(name.isEmpty()) {
            return (Use<T>) typeOf();
        } else {
            final String first = name.first();
            final Map<String, Use<?>> metadataSchema = metadataSchema();
            if(metadataSchema.containsKey(first)) {
                return (Use<T>)metadataSchema.get(first).typeOf(name.withoutFirst());
            } else {
                final Member member = requireMember(first, true);
                return member.typeOf(name.withoutFirst());
            }
        }
    }

    @Override
    default Type javaType(final Name name) {

        if(name.isEmpty()) {
            return TypeFactory.parameterizedClass(Map.class, String.class, Object.class);
        } else {
            final String first = name.first();
            final Map<String, Use<?>> metadataSchema = metadataSchema();
            if(metadataSchema.containsKey(first)) {
                return metadataSchema.get(first).javaType(name.withoutFirst());
            } else {
                final Member member = requireMember(first, true);
                return member.javaType(name.withoutFirst());
            }
        }
    }

    default Map<String, Object> readProperties(final Map<String, Object> object, final Set<Name> expand, final boolean suppress) {

        return readProperties(ValueContext.standardOrSuppressing(suppress), object, expand);
    }

    default Map<String, Object> readProperties(final ValueContext context, final Map<String, Object> object, final Set<Name> expand) {

        final Map<String, Set<Name>> branches = Name.branch(expand);
        final Map<String, Object> result = new HashMap<>();
        getProperties().forEach((k, v) -> result.put(k, v.create(context, object.get(k), branches.get(k))));
        return Collections.unmodifiableMap(result);
    }

    default Map<String, Object> readMeta(final Map<String, Object> object, final boolean suppress) {

        return readMeta(ValueContext.standardOrSuppressing(suppress), object);
    }

    default Map<String, Object> readMeta(final ValueContext context, final Map<String, Object> object) {

        final Map<String, Use<?>> metadataSchema = metadataSchema();
        final HashMap<String, Object> result = new HashMap<>();
        object.forEach((k, v) -> {
            final Use<?> type = metadataSchema.get(k);
            if(type != null) {
                result.put(k, type.create(context, v, null));
            } else if(Reserved.isMeta(k)) {
                result.put(k, v);
            }
        });
        return Collections.unmodifiableMap(result);
    }

    default void serializeProperties(final Map<String, Object> object, final DataOutput out) throws IOException {

        final Map<String, Property> properties = getProperties();
        out.writeInt(properties.size());
        for(final Map.Entry<String, Property> entry : new TreeMap<>(properties).entrySet()) {
            UseString.DEFAULT.serializeValue(entry.getKey(), out);
            final Object value = object.get(entry.getKey());
            entry.getValue().serialize(value, out);
        }
    }

    default String hash(final Map<String, Object> object) {

        try {
            try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                 final DataOutputStream daos = new DataOutputStream(baos)) {
                serializeProperties(object, daos);
                @SuppressWarnings("all")
                final byte[] bytes = Hashing.md5().newHasher().putBytes(baos.toByteArray()).hash().asBytes();
                return BaseEncoding.base64().encode(bytes);
            }
        } catch (final IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    @SuppressWarnings("rawtypes")
    default io.swagger.v3.oas.models.media.Schema<?> openApi() {

        final Map<String, io.swagger.v3.oas.models.media.Schema> properties = new HashMap<>();
        final Set<Name> expand = getExpand();
        final Map<String, Set<Name>> branches = Name.branch(expand);
        metadataSchema().forEach((name, type) -> {
            properties.put(name, type.openApi(branches.get(name)));
        });
        getMembers().forEach((name, member) -> {
            if(!member.isAlwaysHidden()) {
                properties.put(name, member.openApi(branches.get(name)));
            }
        });
        return new io.swagger.v3.oas.models.media.ObjectSchema()
                .properties(properties)
                .description(getDescription())
                .name(getQualifiedName().toString());
    }

    default void expand(final Expander expander, final Set<Name> expand) {

        expand(Name.empty(), expander, expand);
    }

    default void expand(final Name parent, final Expander expander, final Set<Name> expand) {

        if(!expand.isEmpty()) {
            final Map<String, Set<Name>> branches = Name.branch(expand);
            getMembers().forEach((name, member) -> {
                final Set<Name> branch = branches.get(member.getName());
                member.expand(parent.with(name), expander, branch);
            });
        }
    }

    default Instance expand(final Instance object, final Expander expander, final Set<Name> expand) {

        return expand(Name.empty(), object, expander, expand);
    }

    default Instance expand(final Name parent, final Instance object, final Expander expander, final Set<Name> expand) {

        final Map<String, Set<Name>> branches = Name.branch(expand);
        return transformMembers(object, (member, before) -> {
            final Set<Name> branch = branches.get(member.getName());
            return member.expand(parent.with(member.getName()), before, expander, branch);
        });
    }

    default Instance applyVisibility(final Context context, final Instance object) {

        final Context thisContext = context.with(VAR_THIS, object);
        final Set<String> delete = new HashSet<>();
        final Instance tmp = transformMembers(object, (member, before) -> {
            if (member.isVisible(thisContext, before)) {
                return member.applyVisibility(thisContext, before);
            } else {
                delete.add(member.getName());
                return null;
            }
        });
        if(delete.isEmpty()) {
            return tmp;
        } else {
            final Map<String, Object> result = new HashMap<>(tmp);
            delete.forEach(result::remove);
            return new Instance(result);
        }
    }

    default Instance evaluateTransients(final Context context, final Instance object, final Set<Name> expand) {

        final Map<String, Set<Name>> branches = Name.branch(expand);
        final Context thisContext = context.with(VAR_THIS, object);
        return transformMembers(object, (member, value) -> {
            if(branches.containsKey(member.getName())) {
                return member.evaluateTransients(thisContext, value, branches.get(member.getName()));
            } else {
                return value;
            }
        });
    }

    default Instance transformMembers(final Instance object, final BiFunction<Member, Object, Object> fn) {

        final HashMap<String, Object> changed = new HashMap<>();
        getMembers().forEach((name, member) -> {
            final Object before = object.get(name);
            final Object after = fn.apply(member, before);
            // Reference equals is correct behaviour
            if (before != after) {
                changed.put(member.getName(), after);
            }
        });
        if(changed.isEmpty()) {
            return object;
        } else {
            final Map<String, Object> result = new HashMap<>(object);
            result.putAll(changed);
            return new Instance(result);
        }
    }

    static Map<String, Object> deserializeProperties(final DataInput in) throws IOException {

        final Map<String, Object> data = new HashMap<>();
        final int size = in.readInt();
        for(int i = 0; i != size; ++i) {
            final String key = UseString.DEFAULT.deserializeValue(in);
            final Object value = Use.deserializeAny(in);
            data.put(key, value);
        }
        return data;
    }

    default Set<Expression> refQueries(final Name otherSchemaName, final Set<Name> expand) {

        return refQueries(otherSchemaName, expand, Name.empty());
    }

    default Set<Expression> refQueries(final Name otherSchemaName, final Set<Name> expand, final Name name) {

        final Map<String, Set<Name>> branches = Name.branch(expand);
        return getMembers().entrySet().stream()
                .filter(e -> branches.containsKey(e.getKey()))
                .flatMap(e -> e.getValue().refQueries(otherSchemaName, branches.get(e.getKey()), name.with(e.getKey())).stream())
                .collect(Collectors.toSet());
    }

    default Set<Name> refExpand(final Name otherSchemaName, final Set<Name> expand) {

        final Map<String, Set<Name>> branches = Name.branch(expand);
        return getMembers().entrySet().stream()
                .filter(e -> branches.containsKey(e.getKey()))
                .flatMap(e -> e.getValue().refExpand(otherSchemaName, branches.get(e.getKey())).stream())
                .collect(Collectors.toSet());
    }

    default Map<Ref, Long> refVersions(final Instance value) {

        final Map<Ref, Long> versions = new HashMap<>();
        getProperties().forEach((k, v) -> versions.putAll(v.refVersions(value.get(k))));
        return versions;
    }

    default boolean supportsTrivialJoin(final Set<Name> expand) {

        final Map<String, Set<Name>> branches = Name.branch(expand);
        return getMembers().entrySet().stream().allMatch(entry -> {
            final String name = entry.getKey();
            final Member member = entry.getValue();
            final Set<Name> memberExpand = branches.get(name);
            if(memberExpand != null) {
                return member.supportsTrivialJoin(memberExpand);
            } else {
                return true;
            }
        });
    }

    @SuppressWarnings("unchecked")
    default boolean areEqual(final Map<String, Object> a, final Map<String, Object> b) {

        if(a == null || b == null) {
            return a == null && b == null;
        } else {
            for(final Map.Entry<String, Use<?>> entry : metadataSchema().entrySet()) {
                if(!((Use<Object>)entry.getValue()).areEqual(a.get(entry.getKey()), b.get(entry.getKey()))) {
                    return false;
                }
            }
            for(final Map.Entry<String, ? extends Member> entry : getMembers().entrySet()) {
                if(!((Use<Object>)entry.getValue().typeOf()).areEqual(a.get(entry.getKey()), b.get(entry.getKey()))) {
                    return false;
                }
            }
        }
        return true;
    }
}

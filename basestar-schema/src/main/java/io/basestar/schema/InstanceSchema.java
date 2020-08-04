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

import com.fasterxml.jackson.annotation.JsonInclude;
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.schema.exception.UnexpectedTypeException;
import io.basestar.schema.layout.Layout;
import io.basestar.schema.use.Use;
import io.basestar.schema.use.UseInstance;
import io.basestar.schema.use.UseString;
import io.basestar.schema.util.Expander;
import io.basestar.schema.util.Ref;
import io.basestar.util.Name;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

public interface InstanceSchema extends Schema<Instance>, Layout, Member.Resolver, Property.Resolver {

    Instance create(Map<String, Object> value, Set<Name> expand, boolean suppress);

    interface Descriptor extends Schema.Descriptor<Instance> {

        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        Map<String, Property.Descriptor> getProperties();

        @Override
        InstanceSchema build(Resolver.Constructing resolver, Name qualifiedName, int slot);

        @Override
        InstanceSchema build();
    }

    interface Builder extends Schema.Builder<Instance>, Descriptor, Property.Resolver.Builder {

        Builder setDescription(final String description);
    }

    SortedMap<String, Use<?>> metadataSchema();

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

    @Override
    default Map<String, Object> applyLayout(final Set<Name> expand, final Map<String, Object> object) {

        return create(object, expand, true);
    }

    @Override
    default Map<String, Object> unapplyLayout(final Set<Name> expand, final Map<String, Object> object) {

        return create(object, expand, true);
    }

    InstanceSchema getExtend();

    @Override
    UseInstance use();

    String id();

    default Use<?> typeOfId() {

        return metadataSchema().get(id());
    }

    default boolean hasMutableProperties() {

        return !getProperties().values().stream().map(Property::isImmutable).reduce(true, (a, b) -> a && b);
    }

    @Override
    @SuppressWarnings("unchecked")
    default Instance create(final Object value, final Set<Name> expand, final boolean suppress) {

        if(value == null) {
            return null;
        } else if(value instanceof Map) {
            return create((Map<String, Object>) value, expand, suppress);
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

    boolean isConcrete();

    @SuppressWarnings("unchecked")
    default <T> Use<T> typeOf(final Name name) {

        if(name.isEmpty()) {
            throw new IllegalStateException();
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

    default boolean hasParent(final Name qualifiedName) {

        final InstanceSchema extend = getExtend();
        if(extend != null) {
            if(extend.getQualifiedName().equals(qualifiedName)) {
                return true;
            } else {
                return extend.hasParent(qualifiedName);
            }
        } else {
            return false;
        }
    }

    default Map<String, Object> readProperties(final Map<String, Object> object, final Set<Name> expand, final boolean suppress) {

        final Map<String, Set<Name>> branches = Name.branch(expand);
        final Map<String, Object> result = new HashMap<>();
        getProperties().forEach((k, v) -> result.put(k, v.create(object.get(k), branches.get(k), suppress)));
        return Collections.unmodifiableMap(result);
    }

    default Map<String, Object> readMeta(final Map<String, Object> object, final boolean suppress) {

        final Map<String, Use<?>> metadataSchema = metadataSchema();
        final HashMap<String, Object> result = new HashMap<>();
        object.forEach((k, v) -> {
            final Use<?> type = metadataSchema.get(k);
            if(type != null) {
                result.put(k, type.create(v, null, suppress));
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

    @Override
    @SuppressWarnings("rawtypes")
    default io.swagger.v3.oas.models.media.Schema<?> openApi() {

        final Map<String, io.swagger.v3.oas.models.media.Schema> properties = new HashMap<>();
        metadataSchema().forEach((name, type) -> {
            properties.put(name, type.openApi());
        });
        getMembers().forEach((name, member) -> {
            if(!member.isAlwaysHidden()) {
                properties.put(name, member.openApi());
            }
        });
        return new io.swagger.v3.oas.models.media.ObjectSchema()
                .properties(properties)
                .description(getDescription())
                .name(getQualifiedName().toString());
    }

    default Instance expand(final Instance object, final Expander expander, final Set<Name> expand) {

        final Map<String, Set<Name>> branches = Name.branch(expand);
        return transformMembers(object, (member, before) -> {
            final Set<Name> branch = branches.get(member.getName());
            return member.expand(before, expander, branch);
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

    /*private*/ default Instance transformMembers(final Instance object, final BiFunction<Member, Object, Object> fn) {

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

    @Override
    Descriptor descriptor();
}

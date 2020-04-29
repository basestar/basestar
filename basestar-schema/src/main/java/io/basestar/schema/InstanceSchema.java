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

import io.basestar.expression.Context;
import io.basestar.schema.use.Use;
import io.basestar.schema.use.UseString;
import io.basestar.util.Path;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import java.util.function.BiFunction;

public interface InstanceSchema extends Schema<Instance>, Member.Resolver, Property.Resolver {

    interface Builder extends Schema.Builder<Instance> {

        @Override
        InstanceSchema build(Resolver resolver, String name, int slot);
    }

    SortedMap<String, Use<?>> metadataSchema();

    InstanceSchema getExtend();

    default Set<Path> requiredExpand(final Set<Path> paths) {

        final Set<Path> result = new HashSet<>();
        for (final Map.Entry<String, Set<Path>> branch : Path.branch(paths).entrySet()) {
            final Member member = getMember(branch.getKey(), true);
            if(member != null) {
                for(final Path tail : member.requiredExpand(branch.getValue())) {
                    result.add(Path.of(branch.getKey()).with(tail));
                }
            }
        }
        return Path.simplify(result);
    }

    default Set<Path> transientExpand(final Path path, final Set<Path> expand) {

        final Set<Path> transientExpand = new HashSet<>(expand);
        final Map<String, Set<Path>> branch = Path.branch(expand);
        getAllMembers().forEach((name, member) -> {
            if(branch.containsKey(name)) {
                final Set<Path> memberExpand = branch.get(name);
                transientExpand.addAll(member.transientExpand(path.with(name), memberExpand));
            }
        });
        return transientExpand;
    }

    boolean isConcrete();

    default Use<?> typeOf(final Path path) {

        if(path.isEmpty()) {
            throw new IllegalStateException();
        } else {
            final String first = path.first();
            final Member member = requireMember(first, true);
            return member.typeOf(path.withoutFirst());
        }
    }

    default boolean hasParent(final String name) {

        final InstanceSchema extend = getExtend();
        if(extend != null) {
            if(extend.getName().equals(name)) {
                return true;
            } else {
                return extend.hasParent(name);
            }
        } else {
            return false;
        }
    }

    default Map<String, Object> readProperties(final Map<String, Object> object, final boolean expand, final boolean suppress) {

        final Map<String, Object> result = new HashMap<>();
        getAllProperties().forEach((k, v) -> result.put(k, v.create(object.get(k), expand, suppress)));
        return Collections.unmodifiableMap(result);
    }

    default void serializeProperties(final Map<String, Object> object, final DataOutput out) throws IOException {

        final Map<String, Property> properties = getAllProperties();
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
        getAllMembers().forEach((name, member) -> {
            if(!member.isAlwaysHidden()) {
                properties.put(name, member.openApi());
            }
        });
        return new io.swagger.v3.oas.models.media.ObjectSchema()
                .properties(properties)
                .description(getDescription())
                .name(getName());
    }

//    default Instance expand(final Instance object, final Expander expander, final Set<Path> expand) {
//
//        final HashMap<String, Object> changed = new HashMap<>();
//        final Map<String, Set<Path>> branches = Path.branch(expand);
//        final Map<String, ? extends Member> members=  getAllMembers();
//        for (final Map.Entry<String, ? extends Member> entry : members.entrySet()) {
//            final Member member = entry.getValue();
//            final Set<Path> branch = branches.get(entry.getKey());
//            final Object before = object.get(member.getName());
//            final Object after = member.expand(before, expander, branch);
//            // Reference equals is correct behaviour
//            if (before != after) {
//                changed.put(member.getName(), after);
//            }
//        }
//        if(changed.isEmpty()) {
//            return object;
//        } else {
//            final Map<String, Object> result = new HashMap<>(object);
//            result.putAll(changed);
//            return new Instance(result);
//        }
//    }

    default Instance expand(final Instance object, final Expander expander, final Set<Path> expand) {

        final Map<String, Set<Path>> branches = Path.branch(expand);
        return transformMembers(object, (member, before) -> {
            final Set<Path> branch = branches.get(member.getName());
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

    default Instance evaluateTransients(final Context context, final Instance object, final Set<Path> expand) {

        final Map<String, Set<Path>> branches = Path.branch(expand);
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
        getAllMembers().forEach((name, member) -> {
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
}

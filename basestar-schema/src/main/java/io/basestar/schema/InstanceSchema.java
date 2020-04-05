package io.basestar.schema;

import io.basestar.schema.use.Use;
import io.basestar.schema.use.UseString;
import io.basestar.util.Path;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

public interface InstanceSchema extends Schema<Instance>, Member.Resolver, Property.Resolver {

    interface Builder extends Schema.Builder<Instance> {

        @Override
        InstanceSchema build(Resolver.Cyclic resolver, String name, int slot);
    }

    SortedMap<String, Use<?>> metadataSchema();

    InstanceSchema getExtend();

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

    default Map<String, Object> readProperties(final Map<String, Object> object) {

        final Map<String, Object> result = new HashMap<>();
        getAllProperties().forEach((k, v) -> result.put(k, v.create(object.get(k))));
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

    default Instance expand(final Instance object, final Expander expander, final Set<Path> expand) {

        final HashMap<String, Object> changed = new HashMap<>();
        final Map<String, Set<Path>> branches = Path.branch(expand);
        final Map<String, ? extends Member> members=  getAllMembers();
        for (final Map.Entry<String, ? extends Member> entry : members.entrySet()) {
            final Member member = entry.getValue();
            final Set<Path> branch = branches.get(entry.getKey());
            final Object before = object.get(member.getName());
            final Object after = member.expand(before, expander, branch);
            // Reference equals is correct behaviour
            if (before != after) {
                changed.put(member.getName(), after);
            }
        }
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

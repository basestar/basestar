package io.basestar.schema.use;

/*-
 * #%L
 * basestar-schema
 * $Id:$
 * $HeadURL:$
 * %%
 * Copyright (C) 2020 basestar.io
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
import io.basestar.schema.Expander;
import io.basestar.schema.Instance;
import io.basestar.schema.Schema;
import io.basestar.schema.exception.InvalidTypeException;
import io.basestar.util.Path;
import lombok.Data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Set Type
 *
 * <strong>Example</strong>
 * <pre>
 * type:
 *   set: string
 * </pre>
 *
 * @param <T>
 */

@Data
public class UseSet<T> implements Use<Set<T>> {

    public static final String NAME = "set";

    private final Use<T> type;

    public static UseSet<?> from(final Object config) {

        final Use<?> type;
        if(config instanceof String) {
            type = Use.fromConfig(config);
        } else if(config instanceof Map) {
            type = Use.fromConfig(((Map)config).get("type"));
        } else {
            throw new InvalidTypeException();
        }
        return new UseSet<>(type);
    }

    @Override
    public <R> R visit(final Visitor<R> visitor) {

        return visitor.visitSet(this);
    }

    @Override
    public Object toJson() {

        return ImmutableMap.of(
                NAME, type
        );
    }

    @Override
    public UseSet<?> resolve(final Schema.Resolver resolver) {

        return new UseSet<>(this.type.resolve(resolver));
    }

    @Override
    public Use<?> typeOf(final Path path) {

        if(path.isEmpty()) {
            return this;
        } else {
            return type.typeOf(path);
        }
    }

    @Override
    public Set<T> create(final Object value) {

        if(value == null) {
            return null;
        } else if(value instanceof Collection) {
            return ((Collection<?>)value).stream().map(type::create).collect(Collectors.toSet());
        } else {
            throw new InvalidTypeException();
        }
    }

    @Override
    public Code code() {

        return Code.SET;
    }

    @Override
    public void serializeValue(final Set<T> value, final DataOutput out) throws IOException {

        out.writeInt(value.size());
        for(final T v : new TreeSet<>(value)) {
            type.serialize(v, out);
        }
    }

    @Override
    public Set<T> deserializeValue(final DataInput in) throws IOException {

        return deserializeAnyValue(in);
    }

    public static <T> Set<T> deserializeAnyValue(final DataInput in) throws IOException {

        final Set<T> result = new HashSet<>();
        final int size = in.readInt();
        for(int i = 0; i != size; ++i) {
            result.add(Use.deserializeAny(in));
        }
        return result;
    }

    @Override
    public Set<T> expand(final Set<T> value, final Expander expander, final Set<Path> expand) {

        if(value != null) {
            boolean changed = false;
            final Set<T> expanded = new HashSet<>();
            for(final T before : value) {
                final T after = type.expand(before, expander, expand);
                expanded.add(after);
                changed = changed || (before != after);
            }
            return changed ? expanded : value;
        } else {
            return null;
        }
    }

//    @Override
//    public Map<String, Object> openApiType() {
//
//        return ImmutableMap.of(
//                "type", "array",
//                "items", type.openApiType()
//        );
//    }

    @Override
    @Deprecated
    public Set<Path> requireExpand(final Set<Path> paths) {

        return type.requireExpand(paths);
    }

    @Override
    @Deprecated
    public Multimap<Path, Instance> refs(final Set<T> value) {

        final Multimap<Path, Instance> result = HashMultimap.create();
        if(value != null) {
            value.forEach(v -> type.refs(v).forEach(result::put));
        }
        return result;
    }

    @Override
    public String toString() {

        return NAME + "<" + type + ">";
    }
}

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
import io.basestar.expression.Context;
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
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Array Type
 *
 * <strong>Example</strong>
 * <pre>
 * type:
 *   array: string
 * </pre>
 *
 * @param <T>
 */

@Data
public class UseArray<T> implements Use<List<T>> {

    public static final String NAME = "array";

    private final Use<T> type;

    @Override
    public <R> R visit(final Visitor<R> visitor) {

        return visitor.visitArray(this);
    }

    public static UseArray<?> from(final Object config) {

        final Use<?> type;
        if(config instanceof String) {
            type = Use.fromConfig(config);
        } else if(config instanceof Map) {
            type = Use.fromConfig(((Map)config).get("type"));
        } else {
            throw new InvalidTypeException();
        }
        return new UseArray<>(type);
    }

    @Override
    public Object toJson() {

        return ImmutableMap.of(
                NAME, type
        );
    }

    @Override
    public UseArray<?> resolve(final Schema.Resolver resolver) {

        return new UseArray<>(this.type.resolve(resolver));
    }

    @Override
    public List<T> create(final Object value, final boolean expand) {

        if(value == null) {
            return null;
        } else if(value instanceof Collection) {
            return ((Collection<?>)value).stream()
                    .map(v -> type.create(v, expand))
                    .collect(Collectors.toList());
        } else {
            throw new InvalidTypeException();
        }
    }

    @Override
    public Code code() {

        return Code.ARRAY;
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
    public void serializeValue(final List<T> value, final DataOutput out) throws IOException {

        out.writeInt(value.size());
        for(final T v : value) {
            type.serialize(v, out);
        }
    }

    @Override
    public List<T> deserializeValue(final DataInput in) throws IOException {

        return deserializeAnyValue(in);
    }

    public static <T> List<T> deserializeAnyValue(final DataInput in) throws IOException {

        final List<T> result = new ArrayList<>();
        final int size = in.readInt();
        for(int i = 0; i != size; ++i) {
            result.add(Use.deserializeAny(in));
        }
        return result;
    }

//    @Override
//    public List<T> expand(final List<T> value, final Expander expander, final Set<Path> expand) {
//
//        if(value != null) {
//            boolean changed = false;
//            final List<T> expanded = new ArrayList<>();
//            for(final T before : value) {
//                final T after = type.expand(before, expander, expand);
//                expanded.add(after);
//                changed = changed || (before != after);
//            }
//            return changed ? expanded : value;
//        } else {
//            return null;
//        }
//    }

    @Override
    public List<T> expand(final List<T> value, final Expander expander, final Set<Path> expand) {

        return transform(value, before -> type.expand(before, expander, expand));
    }

    @Override
    public List<T> applyVisibility(final Context context, final List<T> value) {

        return transform(value, before -> type.applyVisibility(context, before));
    }

    @Override
    public List<T> evaluateTransients(final Context context, final List<T> value, final Set<Path> expand) {

        return transform(value, before -> type.evaluateTransients(context, before, expand));
    }

    private static <T> List<T> transform(final List<T> value, final Function<T, T> fn) {

        if(value != null) {
            boolean changed = false;
            final List<T> result = new ArrayList<>();
            for(final T before : value) {
                final T after = fn.apply(before);
                result.add(after);
                changed = changed || (before != after);
            }
            return changed ? result : value;
        } else {
            return null;
        }
    }

    @Override
    public Set<Path> transientExpand(final Path path, final Set<Path> expand) {

        return type.transientExpand(path, expand);
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
    public Set<Path> requiredExpand(final Set<Path> paths) {

        return type.requiredExpand(paths);
    }

    @Override
    @Deprecated
    public Multimap<Path, Instance> refs(final List<T> value) {

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
//
//    class Bound<List<T>> {
//
//    }
}

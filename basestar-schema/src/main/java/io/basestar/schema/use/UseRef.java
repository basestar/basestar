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

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import io.basestar.schema.*;
import io.basestar.schema.exception.InvalidTypeException;
import io.basestar.util.Path;
import lombok.Data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Object Type
 *
 * Stores a reference to the object.
 *
 * <strong>Example</strong>
 * <pre>
 * type: MyObject
 * </pre>
 */

@Data
public class UseRef implements Use<Instance> {

    public static final String NAME = "ref";

    @JsonSerialize(using = Named.NameSerializer.class)
    private final ObjectSchema schema;

    @Override
    public <R> R visit(final Visitor<R> visitor) {

        return visitor.visitRef(this);
    }

    public static UseRef from(final ObjectSchema schema, final Object config) {

        return new UseRef(schema);
    }

    @Override
    public Object toJson() {

        return ImmutableMap.of(
                NAME, schema.getName()
        );
    }

    @Override
    public UseRef resolve(final Schema.Resolver resolver) {

        return new UseRef(resolver.requireObjectSchema(schema.getName()));
    }

    @Override
    public Use<?> typeOf(final Path path) {

        if(path.isEmpty()) {
            return this;
        } else {
            return schema.typeOf(path);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public Instance create(final Object value, final boolean expand) {

        if(value == null) {
            return null;
        } else if(value instanceof Map) {
            final Map<String, Object> map = (Map<String, Object>)value;
            final String id = Instance.getId(map);
            if(id == null) {
                return null;
            } else {
                if(expand) {
                    return schema.create(map, true);
                } else {
                    return ObjectSchema.ref(id);
                }
            }
        } else {
            throw new InvalidTypeException();
        }
    }

    @Override
    public Code code() {

        return Code.REF;
    }

    @Override
    public void serializeValue(final Instance value, final DataOutput out) throws IOException {

//        final String schema = Instance.getSchema(value);
        final String id = Instance.getId(value);
//        UseString.DEFAULT.serializeValue(schema, out);
        UseString.DEFAULT.serializeValue(id, out);
    }

    @Override
    public Instance deserializeValue(final DataInput in) throws IOException {

        return deserializeAnyValue(in);
    }

    public static Instance deserializeAnyValue(final DataInput in) throws IOException {

//        final String schema = UseString.deserializeValue(in);
        final String id = UseString.DEFAULT.deserializeValue(in);
        final Map<String, Object> ref = new HashMap<>();
//        Instance.setSchema(ref, schema);
        Instance.setId(ref, id);
        return new Instance(ref);
    }

//    @Override
//    public void serialize(final Instance value, final DataOutput out) throws IOException {
//
//        if(value != null) {
//            final String id = Instance.getId(value);
//            if(id == null) {
//                out.writeInt(Code.NULL.ordinal());
//            } else {
//                out.writeInt(Code.REF.ordinal());
//                out.write(id.getBytes(Charsets.UTF_8));
//            }
//        } else {
//            out.writeInt(Code.NULL.ordinal());
//        }
//    }

    @Override
    public Instance expand(final Instance value, final Expander expander, final Set<Path> expand) {

        if(value != null) {
            if(expand == null) {
                if(value.size() == 1 && value.containsKey(Reserved.ID)) {
                    return value;
                } else {
                    return ObjectSchema.ref(Instance.getId(value));
                }
            } else {
                return expander.ref(schema, value, expand);
            }
        } else {
            return null;
        }
    }

    @Override
    @Deprecated
    public Set<Path> requiredExpand(final Set<Path> paths) {

        final Set<Path> copy = Sets.newHashSet(paths);
        copy.remove(Path.of(Reserved.SCHEMA));
        copy.remove(Path.of(Reserved.ID));

        if(!copy.isEmpty()) {
            final Set<Path> result = Sets.newHashSet();
            result.add(Path.of());
            result.addAll(schema.requiredExpand(paths));
            return result;
        } else {
            return Collections.emptySet();
        }
    }

    @Override
    @Deprecated
    public Multimap<Path, Instance> refs(final Instance value) {

        final Multimap<Path, Instance> result = HashMultimap.create();
        result.put(Path.empty(), value);
        return result;
    }


    @Override
    public String toString() {

        return schema.getName();
    }

//    @Data
//    public static class Unresolved implements Use<Instance> {
//
//        private final String schema;
//
//        public static Unresolved from(final Object config) {
//
//            final String schema;
//            if(config instanceof String) {
//                schema = (String)config;
//            } else if(config instanceof Map) {
//                schema = (String)((Map)config).get("schema");
//            } else {
//                throw new IllegalStateException();
//            }
//            return new Unresolved(schema);
//        }
//
//        @Override
//        public <R> R visit(final Visitor<R> visitor) {
//
//            throw new UnsupportedOperationException();
//        }
//
//        @Override
//        public Use<?> resolve(final Schema.Resolver resolver) {
//
//            return new UseObject(resolver.requireObjectSchema(schema));
//        }
//
//        @Override
//        public Instance create(final Object value) {
//
//            throw new UnsupportedOperationException();
//        }
//
//        @Override
//        public Code code() {
//
//            throw new UnsupportedOperationException();
//        }
//
//        @Override
//        public void serializeValue(final Instance value, final DataOutput out) {
//
//            throw new UnsupportedOperationException();
//        }
//
//        @Override
//        public Instance expand(final Instance value, final Expander expander, final Set<Path> expand) {
//
//            throw new UnsupportedOperationException();
//        }
//
//        @Override
//        public Set<Path> requireExpand(final Set<Path> paths) {
//
//            throw new UnsupportedOperationException();
//        }
//
//        @Override
//        public Multimap<Path, Instance> refs(final Instance value) {
//
//            throw new UnsupportedOperationException();
//        }
//
//        @Override
//        public Object toJson() {
//
//            return schema;
//        }
//    }
}

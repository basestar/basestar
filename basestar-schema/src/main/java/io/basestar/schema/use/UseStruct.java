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
import com.google.common.collect.Multimap;
import io.basestar.schema.Expander;
import io.basestar.schema.Instance;
import io.basestar.schema.Schema;
import io.basestar.schema.StructSchema;
import io.basestar.schema.exception.InvalidTypeException;
import io.basestar.util.Path;
import lombok.Data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * Struct Type
 *
 * Stores a copy of the object, the declared (static) type is used, so properties defined
 * in a subclass of the declared struct type will be lost.
 *
 * For polymorphic storage, an Object type must be used.
 *
 * <strong>Example</strong>
 * <pre>
 * type: MyStruct
 * </pre>
 */

@Data
public class UseStruct implements Use<Instance> {

    private final StructSchema schema;

    public static UseStruct from(final StructSchema schema, final Object config) {

        return new UseStruct(schema);
    }

    @Override
    public <R> R visit(final Visitor<R> visitor) {

        return visitor.visitStruct(this);
    }

    @Override
    public Object toJson() {

        return schema.getName();
    }

    @Override
    public UseStruct resolve(final Schema.Resolver resolver) {

        return this;
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
    public Instance create(final Object value) {

        if(value == null) {
            return null;
        } else if(value instanceof Map) {
            return schema.create((Map<String, Object>)value);
        } else {
            throw new InvalidTypeException();
        }
    }

    @Override
    public Code code() {

        return Code.STRUCT;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void serializeValue(final Instance value, final DataOutput out) throws IOException {

        schema.serialize(value, out);
    }

    @Override
    public Instance deserializeValue(final DataInput in) throws IOException {

        return deserializeAnyValue(in);
    }

    public static Instance deserializeAnyValue(final DataInput in) throws IOException {

        return StructSchema.deserialize(in);
    }

    @Override
    public Instance expand(final Instance value, final Expander expander, final Set<Path> expand) {

        if(value != null) {
            return schema.expand(value, expander, expand);
        } else {
            return null;
        }
    }

//    @Override
//    public Map<String, Object> openApiType() {
//
//        return schema.openApiRef();
//    }

    @Override
    @Deprecated
    public Set<Path> requireExpand(final Set<Path> paths) {

        return schema.requiredExpand(paths);
    }

    @Override
    @Deprecated
    public Multimap<Path, Instance> refs(final Instance value) {

        if(value != null) {
            return schema.refs(value);
        } else {
            return HashMultimap.create();
        }
    }

    @Override
    public String toString() {

        return schema.getName();
    }
}

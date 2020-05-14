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
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.expression.compare.Eq;
import io.basestar.expression.constant.PathConstant;
import io.basestar.schema.*;
import io.basestar.schema.exception.InvalidTypeException;
import io.basestar.util.Path;
import lombok.Data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

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
public class UseRef implements UseInstance {

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
    @SuppressWarnings("unchecked")
    public Instance create(final Object value, final boolean expand, final boolean suppress) {

        if(value == null) {
            return null;
        } else if(value instanceof Map) {
            final Map<String, Object> map = (Map<String, Object>)value;
            final String id = Instance.getId(map);
            if(id == null) {
                return null;
            } else {
                if(expand) {
                    return schema.create(map, true, suppress);
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

        final String id = Instance.getId(value);
        UseString.DEFAULT.serializeValue(id, out);
    }

    @Override
    public Instance deserializeValue(final DataInput in) throws IOException {

        return deserializeAnyValue(in);
    }

    public static Instance deserializeAnyValue(final DataInput in) throws IOException {

        final String id = UseString.DEFAULT.deserializeValue(in);
        final Map<String, Object> ref = new HashMap<>();
        Instance.setId(ref, id);
        return new Instance(ref);
    }

    @Override
    public Instance expand(final Instance value, final Expander expander, final Set<Path> expand) {

        if(value != null) {
            if(expand == null) {
                // If non-expanded, strip back to just a ref, this is needed because expand is also used to
                // reset after expansion for permission evaluation
                if(value.size() == 1 && value.containsKey(Reserved.ID)) {
                    return value;
                } else {
                    return ObjectSchema.ref(Instance.getId(value));
                }
            } else {
                return expander.expandRef(schema, value, expand);
            }
        } else {
            return null;
        }
    }

    @Override
    public Set<Constraint.Violation> validate(final Context context, final Path path, final Instance value) {

        // Not our responsibility to validate another object
        return Collections.emptySet();
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

    @Override
    public Set<Expression> refQueries(final String otherTypeName, final Set<Path> expand, final Path path) {

        final Set<Expression> queries = new HashSet<>();
        if(schema.getName().equals(otherTypeName)) {
            queries.add(new Eq(new PathConstant(path.with(Reserved.ID)), new PathConstant(Path.of(Reserved.THIS, Reserved.ID))));
        }
        if(expand != null && !expand.isEmpty()) {
            queries.addAll(schema.refQueries(otherTypeName, expand, path));
        }
        return queries;
    }

    @Override
    public Set<Path> refExpand(final String otherTypeName, final Set<Path> expand) {

        if(schema.getName().equals(otherTypeName)) {
            return expand;
        } else {
            return Collections.emptySet();
        }
    }

    @Override
    public Map<Ref, Long> refVersions(final Instance value) {

        if(value == null) {
            return Collections.emptyMap();
        } else {
            final String id = value.getId();
            final Long version = value.getVersion();
            if(id == null || version == null) {
                return Collections.emptyMap();
            } else {
                return Collections.singletonMap(Ref.of(schema.getName(), id), version);
            }
        }
    }
}

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

import com.google.common.collect.Sets;
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.expression.compare.Eq;
import io.basestar.expression.constant.NameConstant;
import io.basestar.schema.*;
import io.basestar.schema.exception.UnexpectedTypeException;
import io.basestar.schema.util.Expander;
import io.basestar.schema.util.Ref;
import io.basestar.util.Name;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

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
@Slf4j
public class UseObject implements UseLinkable {

    private final ObjectSchema schema;

    @Override
    public <R> R visit(final Visitor<R> visitor) {

        return visitor.visitObject(this);
    }

    public static UseObject from(final ObjectSchema schema, final Object config) {

        return new UseObject(schema);
    }

    @Override
    public UseObject resolve(final Schema.Resolver resolver) {

        if(schema.isAnonymous()) {
            return this;
        } else {
            final ObjectSchema resolved = resolver.requireObjectSchema(schema.getQualifiedName());
            if(resolved == schema) {
                return this;
            } else {
                return new UseObject(resolved);
            }
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public Instance create(final Object value, final Set<Name> expand, final boolean suppress) {

        if(value instanceof Map) {
            final Map<String, Object> map = (Map<String, Object>)value;
            final String id = Instance.getId(map);
            if (id == null) {
                return null;
            } else {
                if(expand != null) {
                    return schema.create(map, expand, suppress);
                } else {
                    return ObjectSchema.ref(id);
                }
            }
        } else if(suppress) {
            return null;
        } else {
            throw new UnexpectedTypeException(this, value);
        }
    }

    @Override
    public Code code() {

        return Code.OBJECT;
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
    public Instance expand(final Instance value, final Expander expander, final Set<Name> expand) {

        if(value != null) {
            if(expand == null) {
                // If non-expanded, strip back to just a ref, this is needed because expand is also used to
                // reset after expansion for permission evaluation
                if(value.size() == 1 && value.containsKey(ObjectSchema.ID)) {
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
    public Set<Constraint.Violation> validate(final Context context, final Name name, final Instance value) {

        // Not our responsibility to validate another object
        return Collections.emptySet();
    }

    @Override
    @Deprecated
    public Set<Name> requiredExpand(final Set<Name> names) {

        final Set<Name> copy = Sets.newHashSet(names);
        copy.remove(Name.of(ObjectSchema.SCHEMA));
        copy.remove(Name.of(ObjectSchema.ID));

        if(!copy.isEmpty()) {
            final Set<Name> result = Sets.newHashSet();
            result.add(Name.of());
            result.addAll(schema.requiredExpand(names));
            return result;
        } else {
            return Collections.emptySet();
        }
    }

    @Override
    public String toString() {

        return schema.getQualifiedName().toString();
    }

    @Override
    public Set<Expression> refQueries(final Name otherTypeName, final Set<Name> expand, final Name name) {

        final Set<Expression> queries = new HashSet<>();
        if(schema.getQualifiedName().equals(otherTypeName)) {
            queries.add(new Eq(new NameConstant(name.with(ObjectSchema.ID)), new NameConstant(Name.of(Reserved.THIS, ObjectSchema.ID))));
        }
        if(expand != null && !expand.isEmpty()) {
            queries.addAll(schema.refQueries(otherTypeName, expand, name));
        }
        return queries;
    }

    @Override
    public Set<Name> refExpand(final Name otherSchemaName, final Set<Name> expand) {

        if(schema.getQualifiedName().equals(otherSchemaName)) {
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
                return Collections.singletonMap(Ref.of(schema.getQualifiedName(), id), version);
            }
        }
    }
}

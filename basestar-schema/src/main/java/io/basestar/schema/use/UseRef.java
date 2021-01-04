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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.expression.compare.Eq;
import io.basestar.expression.constant.NameConstant;
import io.basestar.expression.type.Coercion;
import io.basestar.schema.*;
import io.basestar.schema.util.Expander;
import io.basestar.schema.util.Ref;
import io.basestar.schema.util.ValueContext;
import io.basestar.util.Name;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
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
@Slf4j
public class UseRef implements UseLinkable {

    public static final String VERSIONED_KEY = "versioned";

    private final ReferableSchema schema;

    private final boolean versioned;

    public UseRef(final ReferableSchema schema) {

        this(schema, false);
    }

    public UseRef(final ReferableSchema schema, final boolean versioned) {

        this.schema = schema;
        this.versioned = versioned;
    }

    @Override
    public <R> R visit(final Visitor<R> visitor) {

        return visitor.visitRef(this);
    }

    public static UseRef from(final ReferableSchema schema, final Object config) {

        final boolean versioned;
        if(config instanceof Map) {
            versioned = Coercion.isTruthy(((Map<?, ?>) config).get(VERSIONED_KEY));
        } else {
            versioned = false;
        }
        return new UseRef(schema, versioned);
    }

    @Override
    public UseRef resolve(final Schema.Resolver resolver) {

        if(schema.isAnonymous()) {
            return this;
        } else {
            final ReferableSchema resolved = resolver.requireReferableSchema(schema.getQualifiedName());
            if(resolved == schema) {
                return this;
            } else {
                return new UseRef(resolved, versioned);
            }
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public Instance create(final ValueContext context, final Object value, final Set<Name> expand) {

        return context.createRef(this, value, expand);
    }

    @Override
    public Code code() {

        return Code.REF;
    }

    @Override
    public Object toConfig(final boolean optional) {

        if(versioned) {
            return UseLinkable.super.toConfig(optional);
        } else {
            return ImmutableMap.of(
                    Use.name(getName().toString(), optional), ImmutableMap.of(
                            VERSIONED_KEY, true
                    )
            );
        }
    }

    @Override
    public void serializeValue(final Instance value, final DataOutput out) throws IOException {

        schema.serialize(value, out);
    }

    @Override
    public Instance deserializeValue(final DataInput in) throws IOException {

        return deserializeAnyValue(in);
    }

    public static Instance deserializeAnyValue(final DataInput in) throws IOException {

        return ReferableSchema.deserialize(in);
    }

    @Override
    public Instance expand(final Name parent, final Instance value, final Expander expander, final Set<Name> expand) {

        if(value != null) {
            if(expand == null) {
                // If non-expanded, strip back to just a ref, this is needed because expand is also used to
                // reset after expansion for permission evaluation
                if(versioned) {
                    if(value.size() == 2 && value.containsKey(ReferableSchema.ID) && value.containsKey(ReferableSchema.VERSION)) {
                        return value;
                    } else {
                        return ReferableSchema.versionedRef(Instance.getId(value), Instance.getVersion(value));
                    }
                } else {
                    if (value.size() == 1 && value.containsKey(ReferableSchema.ID)) {
                        return value;
                    } else {
                        return ReferableSchema.ref(Instance.getId(value));
                    }
                }
            } else if(versioned) {
                return expander.expandVersionedRef(parent, schema, value, expand);
            } else {
                return expander.expandRef(parent, schema, value, expand);
            }
        } else {
            return null;
        }
    }

    @Override
    public void expand(final Name parent, final Expander expander, final Set<Name> expand) {

        if(isVersioned()) {
            expander.expandVersionedRef(parent, schema, null, expand);
        } else {
            expander.expandRef(parent, schema, null, expand);
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
        copy.remove(Name.of(ReferableSchema.SCHEMA));
        copy.remove(Name.of(ReferableSchema.ID));
        if(versioned) {
            copy.remove(Name.of(ReferableSchema.VERSION));
        }

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
    public Instance defaultValue() {

        return schema.create(Collections.emptyMap());
    }

    @Override
    public String toString() {

        return schema.getQualifiedName().toString();
    }

    @Override
    public Set<Expression> refQueries(final Name otherTypeName, final Set<Name> expand, final Name name) {

        final Set<Expression> queries = new HashSet<>();
        if(schema.getQualifiedName().equals(otherTypeName)) {
            queries.add(new Eq(new NameConstant(name.with(ReferableSchema.ID)), new NameConstant(Name.of(Reserved.THIS, ReferableSchema.ID))));
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

    @Override
    public Object[] key(final Instance value) {

        if(value != null) {
            final String id = Instance.getId(value);
            if(isVersioned()) {
                final Long version = Instance.getVersion(value);
                return new Object[]{id, version};
            } else {
                return new Object[]{id};
            }
        } else {
            return new Object[]{null};
        }
    }
}

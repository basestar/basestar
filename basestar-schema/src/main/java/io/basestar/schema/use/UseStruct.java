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
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.schema.Constraint;
import io.basestar.schema.Instance;
import io.basestar.schema.Schema;
import io.basestar.schema.StructSchema;
import io.basestar.schema.exception.TypeSyntaxException;
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
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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
@Slf4j
public class UseStruct implements UseInstance {

    public static final String NAME = "struct";

    private final StructSchema schema;

    public static UseStruct from(final StructSchema schema, final Object config) {

        return new UseStruct(schema);
    }

    public static UseStruct from(final Map<String, ?> schema) {

        return new UseStruct(StructSchema.from(schema));
    }

    @SuppressWarnings("unchecked")
    public static UseStruct from(final Object config) {

        if(config instanceof Map) {
            return from((Map<String, ?>)config);
        } else {
            throw new TypeSyntaxException();
        }
    }

    @Override
    public UseStruct resolve(final Schema.Resolver resolver) {

        if(schema.isAnonymous()) {
            return this;
        } else {
            final StructSchema resolved = resolver.requireStructSchema(schema.getQualifiedName());
            if(resolved == schema) {
                return this;
            } else {
                return new UseStruct(resolved);
            }
        }
    }

    @Override
    public <R> R visit(final Visitor<R> visitor) {

        return visitor.visitStruct(this);
    }

    @Override
    public Instance create(final ValueContext context, final Object value, final Set<Name> expand) {

        return context.createStruct(this, value, expand);
    }

    @Override
    public Code code() {

        return Code.STRUCT;
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

        return StructSchema.deserialize(in);
    }

    @Override
    public Instance expand(final Name parent, final Instance value, final Expander expander, final Set<Name> expand) {

        if(value != null) {
            return schema.expand(parent, value, expander, expand);
        } else {
            return null;
        }
    }

    @Override
    public void expand(final Name parent, final Expander expander, final Set<Name> expand) {

        schema.expand(parent, expander, expand);
    }

    @Override
    public Set<Constraint.Violation> validate(final Context context, final Name name, final Instance value) {

        if(value == null) {
            return Collections.emptySet();
        } else {
            return schema.validate(context, name, value);
        }
    }

    @Override
    public Set<Expression> refQueries(final Name otherSchemaName, final Set<Name> expand, final Name name) {

        return schema.refQueries(otherSchemaName, expand, name);
    }

    @Override
    public Set<Name> refExpand(final Name otherSchemaName, final Set<Name> expand) {

        return schema.refExpand(otherSchemaName, expand);
    }

    @Override
    public Map<Ref, Long> refVersions(final Instance value) {

        if(value == null) {
            return Collections.emptyMap();
        }
        return schema.refVersions(value);
    }

    @Override
    @Deprecated
    public Set<Name> requiredExpand(final Set<Name> names) {

        return schema.requiredExpand(names);
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
    public Object toConfig(final boolean optional) {

        if(schema.isAnonymous()) {
            return ImmutableMap.of(
                    Use.name(NAME, optional), schema.getProperties().entrySet().stream().collect(Collectors.toMap(
                            Map.Entry::getKey,
                            e -> e.getValue().typeOf().toConfig()
                    ))
            );
        } else {
            return UseInstance.super.toConfig(optional);
        }
    }
}

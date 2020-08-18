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
import io.basestar.schema.Constraint;
import io.basestar.schema.EnumSchema;
import io.basestar.schema.Schema;
import io.basestar.schema.exception.TypeSyntaxException;
import io.basestar.util.Name;
import lombok.Data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

/**
 * Enum Type
 *
 * <strong>Example</strong>
 * <pre>
 * type: MyEnum
 * </pre>
 */

@Data
public class UseEnum implements UseStringLike<String>, UseNamed<String> {

    public static final String NAME = "enum";

    private final EnumSchema schema;

    public Name getName() {

        return schema.getQualifiedName();
    }

    @Override
    public UseEnum resolve(final Schema.Resolver resolver) {

        if(schema.isAnonymous()) {
            return this;
        } else {
            final EnumSchema resolved = resolver.requireEnumSchema(schema.getQualifiedName());
            if(resolved == schema) {
                return this;
            } else {
                return new UseEnum(resolved);
            }
        }
    }

    @Override
    public <R> R visit(final Visitor<R> visitor) {

        return visitor.visitEnum(this);
    }

    public static UseEnum from(final EnumSchema type, final Object config) {

        return new UseEnum(type);
    }

    public static UseEnum from(final Object config) {

        if(config instanceof List) {
            final List<String> values = new ArrayList<>();
            for(final Object value : (List<?>)config) {
                if(value instanceof String) {
                    values.add((String)value);
                } else {
                    throw new TypeSyntaxException();
                }
            }
            return new UseEnum(EnumSchema.builder()
                    .setValues(values)
                    .build());
        } else {
            throw new TypeSyntaxException();
        }
    }

    @Override
    public String create(final Object value, final Set<Name> expand, final boolean suppress) {

        return schema.create(value, expand, suppress);
    }

    @Override
    public Code code() {

        return Code.ENUM;
    }

    @Override
    public void serializeValue(final String value, final DataOutput out) throws IOException {

        UseString.DEFAULT.serializeValue(value, out);
    }

    @Override
    public String deserializeValue(final DataInput in) throws IOException {

        return deserializeAnyValue(in);
    }

    public static String deserializeAnyValue(final DataInput in) throws IOException {

        return UseString.DEFAULT.deserializeValue(in);
    }

    @Override
    public String toString() {

        return schema.getQualifiedName().toString();
    }

    @Override
    public Set<Constraint.Violation> validate(final Context context, final Name name, final String value) {

        if(value == null) {
            return Collections.emptySet();
        } else {
            return schema.validate(context, name, value);
        }
    }

    @Override
    public void collectDependencies(final Set<Name> expand, final Map<Name, Schema<?>> out) {

        final Schema<?> schema = getSchema();
        out.put(schema.getQualifiedName(), schema);
    }

    @Override
    public Object toConfig() {

        if(schema.isAnonymous()) {
            return ImmutableMap.of(
                    NAME, schema.getValues()
            );
        } else {
            return UseNamed.super.toConfig();
        }
    }

    @Override
    public io.swagger.v3.oas.models.media.Schema<?> openApi(final Set<Name> expand) {

        if(schema.isAnonymous()) {
            return new io.swagger.v3.oas.models.media.StringSchema()._enum(schema.getValues());
        } else {
            return UseNamed.super.openApi(expand);
        }
    }
}

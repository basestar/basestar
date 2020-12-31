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


import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.schema.*;
import io.basestar.schema.exception.MissingSchemaException;
import io.basestar.schema.util.Expander;
import io.basestar.schema.util.Ref;
import io.basestar.schema.util.ValueContext;
import io.basestar.util.Name;
import lombok.Data;

import java.io.DataInput;
import java.io.DataOutput;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public interface UseNamed<T> extends Use<T> {

    Name getName();

    static Lazy from(final String name) {

        return from(name, null);
    }

    static Lazy from(final String name, final Object config) {

        return new Lazy(Name.parse(name), config);
    }

    @Override
    default Object toConfig(final boolean optional) {

        return Use.name(getName().toString(), optional);
    }

    @Override
    default io.swagger.v3.oas.models.media.Schema<?> openApi(final Set<Name> expand) {

        return new io.swagger.v3.oas.models.media.ObjectSchema().$ref(getName().toString());
    }

    @Data
    class Lazy implements UseNamed<Object> {

        private final Name name;

        private final Object config;

        @Override
        public <R> R visit(final Visitor<R> visitor) {

            throw new UnsupportedOperationException();
        }

        @Override
        public Use<?> resolve(final Schema.Resolver resolver) {

            final Schema<?> schema = resolver.requireSchema(name);
            if(schema instanceof EnumSchema) {
                return UseEnum.from((EnumSchema) schema, config);
            } else if(schema instanceof StructSchema) {
                return UseStruct.from((StructSchema) schema, config);
            } else if(schema instanceof ReferableSchema) {
                return UseRef.from((ReferableSchema) schema, config);
            } else {
                throw new MissingSchemaException(name);
            }
        }

        @Override
        public Object create(final ValueContext context, final Object value, final Set<Name> expand) {

            throw new UnsupportedOperationException();
        }

        @Override
        public Code code() {

            throw new UnsupportedOperationException();
        }

        @Override
        public Use<?> typeOf(final Name name) {

            throw new UnsupportedOperationException();
        }

        @Override
        public Type javaType(final Name name) {

            throw new UnsupportedOperationException();
        }

        @Override
        @Deprecated
        public Set<Name> requiredExpand(final Set<Name> names) {

            throw new UnsupportedOperationException();
        }

        @Override
        public Object defaultValue() {

            throw new UnsupportedOperationException();
        }

        @Override
        public Object expand(final Name parent, final Object value, final Expander expander, final Set<Name> expand) {

            throw new UnsupportedOperationException();
        }

        @Override
        public void expand(final Name parent, final Expander expander, final Set<Name> expand) {

            throw new UnsupportedOperationException();
        }

        @Override
        public void serializeValue(final Object value, final DataOutput out) {

            throw new UnsupportedOperationException();
        }

        @Override
        public Object deserializeValue(final DataInput in) {

            throw new UnsupportedOperationException();
        }

        @Override
        public Object applyVisibility(final Context context, final Object value) {

            throw new UnsupportedOperationException();
        }

        @Override
        public Object evaluateTransients(final Context context, final Object value, final Set<Name> expand) {

            throw new UnsupportedOperationException();
        }

        @Override
        public String toString() {

            return name.toString();
        }

        @Override
        public String toString(final Object value) {

            return Objects.toString(value);
        }

        @Override
        public Set<Name> transientExpand(final Name name, final Set<Name> expand) {

            throw new UnsupportedOperationException();
        }

        @Override
        public Set<Constraint.Violation> validate(final Context context, final Name name, final Object value) {

            throw new UnsupportedOperationException();
        }

        @Override
        public io.swagger.v3.oas.models.media.Schema<?> openApi(final Set<Name> expand) {

            return new io.swagger.v3.oas.models.media.ObjectSchema().$ref(name.toString());
        }

        @Override
        public Set<Expression> refQueries(final Name otherSchemaName, final Set<Name> expand, final Name name) {

            throw new UnsupportedOperationException();
        }

        @Override
        public Set<Name> refExpand(final Name otherSchemaName, final Set<Name> expand) {

            throw new UnsupportedOperationException();
        }

        @Override
        public Map<Ref, Long> refVersions(final Object value) {

            throw new UnsupportedOperationException();
        }

        @Override
        public void collectDependencies(final Set<Name> expand, final Map<Name, Schema<?>> out) {

            throw new UnsupportedOperationException();
        }

        @Override
        public boolean areEqual(final Object a, final Object b) {

            throw new UnsupportedOperationException();
        }
    }
}

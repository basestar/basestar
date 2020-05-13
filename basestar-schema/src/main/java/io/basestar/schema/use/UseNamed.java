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


import com.google.common.collect.Multimap;
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.schema.*;
import io.basestar.schema.exception.MissingTypeException;
import io.basestar.util.Path;
import lombok.Data;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.Map;
import java.util.Set;

public interface UseNamed<T> extends Use<T> {

    String getName();

    static Lazy from(final String name) {

        return from(name, null);
    }

    static Lazy from(final String name, final Object config) {

        return new Lazy(name, config);
    }

    @Override
    default Object toJson() {

        return getName();
    }

    @Override
    default io.swagger.v3.oas.models.media.Schema<?> openApi() {

        return new io.swagger.v3.oas.models.media.ObjectSchema().$ref(getName());
    }

    @Data
    class Lazy implements UseNamed<Object> {

        private final String name;

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
            } else if(schema instanceof ObjectSchema) {
                return UseRef.from((ObjectSchema) schema, config);
            } else {
                throw new MissingTypeException(name);
            }
        }

        @Override
        public Object create(final Object value, final boolean expand, final boolean suppress) {

            throw new UnsupportedOperationException();
        }

        @Override
        public Code code() {

            throw new UnsupportedOperationException();
        }

        @Override
        public Use<?> typeOf(final Path path) {

            throw new UnsupportedOperationException();
        }

        @Override
        @Deprecated
        public Set<Path> requiredExpand(final Set<Path> paths) {

            throw new UnsupportedOperationException();
        }

        @Override
        @Deprecated
        public Multimap<Path, Instance> refs(final Object value) {

            throw new UnsupportedOperationException();
        }

        @Override
        public Object expand(final Object value, final Expander expander, final Set<Path> expand) {

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
        public Object evaluateTransients(final Context context, final Object value, final Set<Path> expand) {

            throw new UnsupportedOperationException();
        }

        @Override
        public String toString() {

            return name;
        }

        @Override
        public Set<Path> transientExpand(final Path path, final Set<Path> expand) {

            throw new UnsupportedOperationException();
        }

        @Override
        public Set<Constraint.Violation> validate(final Context context, final Path path, final Object value) {

            throw new UnsupportedOperationException();
        }

        @Override
        public io.swagger.v3.oas.models.media.Schema<?> openApi() {

            return new io.swagger.v3.oas.models.media.ObjectSchema().$ref(name);
        }

        @Override
        public Set<Expression> refQueries(final String otherTypeName, final Path path) {

            throw new UnsupportedOperationException();
        }

        @Override
        public Map<Ref, Long> refVersions(final Object value) {

            throw new UnsupportedOperationException();
        }
    }
}

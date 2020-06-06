package io.basestar.mapper.internal;

/*-
 * #%L
 * basestar-mapper
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

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import io.basestar.schema.use.*;
import io.basestar.type.TypeContext;
import lombok.RequiredArgsConstructor;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

public interface TypeMapper {

    Use<?> use();

    Object unmarshall(Object value);

    Object marshall(Object value);

    default <T> T unmarshall(Object value, Class<T> to) {

        // FIXME:
        return to.cast(unmarshall(value));
    }

    static TypeMapper from(final TypeContext context) {

        final Class<?> erased = context.erasedType();
        if(boolean.class.isAssignableFrom(erased) || Boolean.class.isAssignableFrom(erased)) {
            return new OfBoolean(context);
        } else if(short.class.isAssignableFrom(erased) || int.class.isAssignableFrom(erased) || long.class.isAssignableFrom(erased)
            || Short.class.isAssignableFrom(erased) || Integer.class.isAssignableFrom(erased) || Long.class.isAssignableFrom(erased)) {
            return new OfInteger(context);
        } else if(float.class.isAssignableFrom(erased) || double.class.isAssignableFrom(erased)
                || Float.class.isAssignableFrom(erased) || Double.class.isAssignableFrom(erased)) {
            return new OfNumber(context);
        } else if(String.class.isAssignableFrom(erased)) {
            return new OfString(context);
        } else if(Set.class.isAssignableFrom(erased)) {
            final TypeContext setContext = context.find(Set.class);
            final TypeContext valueType = setContext.typeParameters().get(0).type();
            return new OfSet(context, from(valueType));
        } else if(Collection.class.isAssignableFrom(erased)) {
            final TypeContext collectionContext = context.find(Collection.class);
            final TypeContext valueType = collectionContext.typeParameters().get(0).type();
            return new OfArray(context, from(valueType));
        } else if(erased.isArray()) {
            // FIXME
            throw new UnsupportedOperationException();
        } else if(Map.class.isAssignableFrom(erased)){
            final TypeContext mapContext = context.find(Map.class);
            final TypeContext valueType = mapContext.typeParameters().get(1).type();
            return new OfMap(context, from(valueType));
        } else if(LocalDate.class.isAssignableFrom(erased)){
            return new OfDate(context);
        } else if(LocalDateTime.class.isAssignableFrom(erased)){
            return new OfDateTime(context);
        } else if(Map.class.isAssignableFrom(erased)){
            final TypeContext mapContext = context.find(Map.class);
            final TypeContext valueType = mapContext.typeParameters().get(1).type();
            return new OfMap(context, from(valueType));
        } else {
            return new OfCustom(context);
        }
    }

    @RequiredArgsConstructor
    class OfBoolean implements TypeMapper {

        private final TypeContext context;

        @Override
        public Use<?> use() {

            return UseBoolean.DEFAULT;
        }

        @Override
        public Object unmarshall(final Object value) {

            return UseBoolean.DEFAULT.create(value);
        }

        @Override
        public Object marshall(final Object value) {

            return UseBoolean.DEFAULT.create(value);
        }
    }

    @RequiredArgsConstructor
    class OfInteger implements TypeMapper {

        private final TypeContext context;

        @Override
        public Use<?> use() {

            return UseInteger.DEFAULT;
        }

        @Override
        public Object unmarshall(final Object value) {

            return UseInteger.DEFAULT.create(value);
        }

        @Override
        public Object marshall(final Object value) {

            return UseInteger.DEFAULT.create(value);
        }
    }

    @RequiredArgsConstructor
    class OfNumber implements TypeMapper {

        private final TypeContext context;

        @Override
        public Use<?> use() {

            return UseNumber.DEFAULT;
        }

        @Override
        public Object unmarshall(final Object value) {

            return UseNumber.DEFAULT.create(value);
        }

        @Override
        public Object marshall(final Object value) {

            return UseNumber.DEFAULT.create(value);
        }
    }

    @RequiredArgsConstructor
    class OfString implements TypeMapper {

        private final TypeContext context;

        @Override
        public Use<?> use() {

            return UseString.DEFAULT;
        }

        @Override
        public Object unmarshall(final Object value) {

            return UseString.DEFAULT.create(value);
        }

        @Override
        public Object marshall(final Object value) {

            return UseString.DEFAULT.create(value);
        }
    }

    @RequiredArgsConstructor
    class OfArray implements TypeMapper {

        private final TypeContext context;

        final TypeMapper value;

        @Override
        public Use<?> use() {

            return new UseArray<>(value.use());
        }

        @Override
        public Object unmarshall(final Object value) {

            return UseArray.create(value, false, this.value::marshall);
        }

        @Override
        public Object marshall(final Object value) {

            return UseArray.create(value, false, this.value::marshall);
        }
    }

    @RequiredArgsConstructor
    class OfSet implements TypeMapper {

        private final TypeContext context;

        final TypeMapper value;

        @Override
        public Use<?> use() {

            return new UseSet<>(value.use());
        }

        @Override
        public Object unmarshall(final Object value) {

            return UseSet.create(value, false, this.value::marshall);
        }

        @Override
        public Object marshall(final Object value) {

            return UseSet.create(value, false, this.value::marshall);
        }
    }

    @RequiredArgsConstructor
    class OfMap implements TypeMapper {

        private final TypeContext context;

        final TypeMapper value;

        @Override
        public Use<?> use() {

            return new UseMap<>(value.use());
        }

        @Override
        public Object unmarshall(final Object value) {

            return UseMap.create(value, false, this.value::marshall);
        }

        @Override
        public Object marshall(final Object value) {

            return UseMap.create(value, false, this.value::marshall);
        }
    }

    @RequiredArgsConstructor
    class OfCustom implements TypeMapper {

        private final TypeContext context;

        private final Supplier<SchemaMapper<?, ?>> mapper;

        public OfCustom(final TypeContext context) {

            this.context = context;
            this.mapper =  Suppliers.memoize(() -> SchemaMapper.mapper(context));
        }

        @Override
        public Use<?> use() {

            return UseNamed.from(mapper.get().name());
        }

        @Override
        @SuppressWarnings("unchecked")
        public Object unmarshall(final Object value) {

            return ((SchemaMapper<Object, ?>)mapper.get()).unmarshall(value);
        }

        @Override
        public Object marshall(final Object value) {

            return mapper.get().marshall(value);
        }
    }

    @RequiredArgsConstructor
    class OfDate implements TypeMapper {

        private final TypeContext context;

        @Override
        public Use<?> use() {

            return UseDate.DEFAULT;
        }

        @Override
        public Object unmarshall(final Object value) {

            return UseDate.DEFAULT.create(value);
        }

        @Override
        public Object marshall(final Object value) {

            return UseDate.DEFAULT.create(value);
        }
    }

    @RequiredArgsConstructor
    class OfDateTime implements TypeMapper {

        private final TypeContext context;

        @Override
        public Use<?> use() {

            return UseDateTime.DEFAULT;
        }

        @Override
        public Object unmarshall(final Object value) {

            return UseDateTime.DEFAULT.create(value);
        }

        @Override
        public Object marshall(final Object value) {

            return UseDateTime.DEFAULT.create(value);
        }
    }
}

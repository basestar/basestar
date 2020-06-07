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

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableSet;
import io.basestar.expression.type.Coercion;
import io.basestar.expression.type.Numbers;
import io.basestar.mapper.MappingContext;
import io.basestar.mapper.SchemaMapper;
import io.basestar.schema.use.*;
import io.basestar.type.TypeContext;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.function.Supplier;

public interface TypeMapper {

    Use<?> use();

    Object unmarshall(Object value);

    Object marshall(Object value);

    static TypeMapper from(final MappingContext context, final TypeContext type) {

        final Class<?> erased = type.erasedType();
        if(boolean.class.isAssignableFrom(erased) || Boolean.class.isAssignableFrom(erased)) {
            return new OfBoolean(type);
        } else if(Numbers.isIntegerType(erased)) {
            return new OfInteger(type);
        } else if(Numbers.isRealType(erased)) {
            return new OfNumber(type);
        } else if(String.class.isAssignableFrom(erased)) {
            return new OfString(type);
        } else if(Set.class.isAssignableFrom(erased)) {
            final TypeContext setContext = type.find(Set.class);
            final TypeContext valueType = setContext.typeParameters().get(0).type();
            return new OfSet(type, from(context, valueType));
        } else if(Collection.class.isAssignableFrom(erased)) {
            final TypeContext collectionContext = type.find(Collection.class);
            final TypeContext valueType = collectionContext.typeParameters().get(0).type();
            return new OfArray(type, from(context, valueType));
        } else if(erased.isArray()) {
            if(byte[].class.isAssignableFrom(erased)) {
                return new OfBinary(type);
            } else {
                throw new UnsupportedOperationException();
            }
        } else if(Map.class.isAssignableFrom(erased)){
            final TypeContext mapContext = type.find(Map.class);
            final TypeContext valueType = mapContext.typeParameters().get(1).type();
            return new OfMap(type, from(context, valueType));
        } else if(LocalDate.class.isAssignableFrom(erased)){
            return new OfDate(type);
        } else if(LocalDateTime.class.isAssignableFrom(erased)){
            return new OfDateTime(type);
        } else if(Map.class.isAssignableFrom(erased)){
            final TypeContext mapContext = type.find(Map.class);
            final TypeContext valueType = mapContext.typeParameters().get(1).type();
            return new OfMap(type, from(context, valueType));
        } else {
            return new OfCustom(context, type);
        }
    }

    Set<Class<?>> dependencies();

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

            return Coercion.toBoolean(value);
        }

        @Override
        public Set<Class<?>> dependencies() {

            return Collections.emptySet();
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

            final Long v = Coercion.toLong(value);
            return Numbers.coerce(v, context.erasedType());
        }

        @Override
        public Set<Class<?>> dependencies() {

            return Collections.emptySet();
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

            final Double v = Coercion.toDouble(value);
            return Numbers.coerce(v, context.erasedType());
        }

        @Override
        public Set<Class<?>> dependencies() {

            return Collections.emptySet();
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

            return Coercion.toString(value);
        }

        @Override
        public Set<Class<?>> dependencies() {

            return Collections.emptySet();
        }
    }

    @RequiredArgsConstructor
    class OfArray implements TypeMapper {

        private final TypeContext context;

        @Getter
        private final TypeMapper value;

        @Override
        public Use<?> use() {

            return new UseArray<>(value.use());
        }

        @Override
        public Object unmarshall(final Object value) {

            return UseArray.create(value, false, this.value::unmarshall);
        }

        @Override
        public Object marshall(final Object value) {

            return Coercion.toList(value, context.erasedType(), this.value::marshall);
        }

        @Override
        public Set<Class<?>> dependencies() {

            return value.dependencies();
        }
    }

    @RequiredArgsConstructor
    class OfSet implements TypeMapper {

        private final TypeContext context;

        @Getter
        private final TypeMapper value;

        @Override
        public Use<?> use() {

            return new UseSet<>(value.use());
        }

        @Override
        public Object unmarshall(final Object value) {

            return UseSet.create(value, false, this.value::unmarshall);
        }

        @Override
        public Object marshall(final Object value) {

            return Coercion.toSet(value, context.erasedType(), this.value::marshall);
        }

        @Override
        public Set<Class<?>> dependencies() {

            return value.dependencies();
        }
    }

    @RequiredArgsConstructor
    class OfMap implements TypeMapper {

        private final TypeContext context;

        @Getter
        private final TypeMapper value;

        @Override
        public Use<?> use() {

            return new UseMap<>(value.use());
        }

        @Override
        public Object unmarshall(final Object value) {

            return UseMap.create(value, false, this.value::unmarshall);
        }

        @Override
        public Object marshall(final Object value) {

            return Coercion.toMap(value, context.erasedType(), Objects::toString, this.value::marshall);
        }

        @Override
        public Set<Class<?>> dependencies() {

            return value.dependencies();
        }
    }

    @RequiredArgsConstructor
    class OfCustom implements TypeMapper {

        private final TypeContext context;

        private final Supplier<SchemaMapper<?, ?>> mapper;

        public OfCustom(final MappingContext mappingContext, final TypeContext context) {

            this.context = context;
            this.mapper =  Suppliers.memoize(() -> mappingContext.schemaMapper(context.erasedType()));
        }

        public SchemaMapper<?, ?> getMapper() {

            return mapper.get();
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

        @Override
        public Set<Class<?>> dependencies() {

            return ImmutableSet.of(context.erasedType());
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

            return Coercion.toDate(value);
        }

        @Override
        public Set<Class<?>> dependencies() {

            return Collections.emptySet();
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

            return Coercion.toDateTime(value);
        }

        @Override
        public Set<Class<?>> dependencies() {

            return Collections.emptySet();
        }
    }

    @RequiredArgsConstructor
    class OfBinary implements TypeMapper {

        private final TypeContext context;

        @Override
        public Use<?> use() {

            return UseBinary.DEFAULT;
        }

        @Override
        public Object unmarshall(final Object value) {

            return UseBinary.DEFAULT.create(value);
        }

        @Override
        public Object marshall(final Object value) {

            return Coercion.toBinary(value);
        }

        @Override
        public Set<Class<?>> dependencies() {

            return Collections.emptySet();
        }
    }
}

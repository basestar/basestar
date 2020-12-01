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
import com.google.common.collect.ImmutableSet;
import io.basestar.expression.Expression;
import io.basestar.expression.type.Coercion;
import io.basestar.expression.type.Numbers;
import io.basestar.mapper.MappingContext;
import io.basestar.mapper.SchemaMapper;
import io.basestar.schema.use.*;
import io.basestar.type.TypeContext;
import io.basestar.util.Name;
import io.basestar.util.Sort;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.io.Serializable;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.function.Function;

// Make generic TypeMapper<I, O> (but have to make consistent with weird SchemaMapper<T, O> generic order)

public interface TypeMapper extends Serializable {

    Use<?> use();

    Object unmarshall(Object value);

    Object marshall(Object value);

    Class<?> erasedType();

    Set<Class<?>> dependencies();

    // FIXME: support a wider variety of date/datetime types

    static TypeMapper fromDefault(final MappingContext context, final TypeContext type) {

        final Class<?> erased = type.erasedType();
        if (boolean.class.isAssignableFrom(erased) || Boolean.class.isAssignableFrom(erased)) {
            return new OfBoolean();
        } else if (Numbers.isIntegerType(erased)) {
            return new OfInteger(type.erasedType());
        } else if (Numbers.isRealType(erased)) {
            return new OfNumber(type.erasedType());
        } else if (String.class.isAssignableFrom(erased)) {
            return new OfString();
        } else if (Set.class.isAssignableFrom(erased)) {
            final TypeContext setContext = type.find(Set.class);
            final TypeContext valueType = setContext.typeParameters().get(0).type();
            return new OfSet(type.erasedType(), context.typeMapper(valueType));
        } else if (Collection.class.isAssignableFrom(erased)) {
            final TypeContext collectionContext = type.find(Collection.class);
            final TypeContext valueType = collectionContext.typeParameters().get(0).type();
            return new OfArray(type.erasedType(), context.typeMapper(valueType));
        } else if (erased.isArray()) {
            if (byte[].class.isAssignableFrom(erased)) {
                return new OfBinary(type.erasedType());
            } else {
                throw new UnsupportedOperationException();
            }
        } else if (Map.class.isAssignableFrom(erased)) {
            final TypeContext mapContext = type.find(Map.class);
            final TypeContext valueType = mapContext.typeParameters().get(1).type();
            return new OfMap(type.erasedType(), context.typeMapper(valueType));
        } else if (LocalDate.class.isAssignableFrom(erased)) {
            return new OfDate(type.erasedType());
        } else if (LocalDateTime.class.isAssignableFrom(erased)) {
            return new OfLocalDateTime();
        } else if (Instant.class.isAssignableFrom(erased)) {
            return new OfInstant();
        } else if (Object.class.equals(erased)) {
            return new OfAny();
        } else if (Name.class.equals(erased)) {
            return new OfStringConverted<>(Name.class, Name::parse);
        } else if (Expression.class.equals(erased)) {
            return new OfStringConverted<>(Expression.class, Expression::parse);
        } else if (Sort.class.equals(erased)) {
            return new OfStringConverted<>(Sort.class, Sort::parse);
        } else if (Optional.class.equals(erased)) {
            final TypeContext optionalContext = type.find(Optional.class);
            final TypeContext valueType = optionalContext.typeParameters().get(0).type();
            // FIXME?
            return fromDefault(context, valueType);
        } else {
            return new OfCustom(context, type.erasedType());
        }
    }

    @RequiredArgsConstructor
    class OfBoolean implements TypeMapper {

        @Override
        public Use<?> use() {

            return UseBoolean.DEFAULT;
        }

        @Override
        public Boolean unmarshall(final Object value) {

            return UseBoolean.DEFAULT.create(value);
        }

        @Override
        public Boolean marshall(final Object value) {

            return Coercion.toBoolean(value);
        }

        @Override
        public Class<?> erasedType() {

            return Boolean.class;
        }

        @Override
        public Set<Class<?>> dependencies() {

            return Collections.emptySet();
        }
    }

    @RequiredArgsConstructor
    class OfInteger implements TypeMapper {

        private final Class<?> erasedType;

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
            return Numbers.coerce(v, erasedType);
        }

        @Override
        public Class<?> erasedType() {

            return erasedType;
        }

        @Override
        public Set<Class<?>> dependencies() {

            return Collections.emptySet();
        }
    }

    @RequiredArgsConstructor
    class OfNumber implements TypeMapper {

        private final Class<?> erasedType;

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
            return Numbers.coerce(v, erasedType);
        }

        @Override
        public Class<?> erasedType() {

            return erasedType;
        }

        @Override
        public Set<Class<?>> dependencies() {

            return Collections.emptySet();
        }
    }

    @RequiredArgsConstructor
    class OfString implements TypeMapper {

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
        public Class<?> erasedType() {

            return String.class;
        }

        @Override
        public Set<Class<?>> dependencies() {

            return Collections.emptySet();
        }
    }

    @RequiredArgsConstructor
    class OfArray implements TypeMapper {

        private final Class<? extends List<?>> erasedType;

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

            return Coercion.toList(value, erasedType, this.value::marshall);
        }

        @Override
        public Class<?> erasedType() {

            return erasedType;
        }

        @Override
        public Set<Class<?>> dependencies() {

            return value.dependencies();
        }
    }

    @RequiredArgsConstructor
    class OfSet implements TypeMapper {

        private final Class<? extends Set<?>> erasedType;

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

            return Coercion.toSet(value, erasedType, this.value::marshall);
        }

        @Override
        public Class<?> erasedType() {

            return erasedType;
        }

        @Override
        public Set<Class<?>> dependencies() {

            return value.dependencies();
        }
    }

    @RequiredArgsConstructor
    class OfMap implements TypeMapper {

        private final Class<? extends Map<?, ?>> erasedType;

        @Getter
        private final TypeMapper value;

        @Override
        public Use<?> use() {

            return new UseMap<>(value.use());
        }

        @Override
        public Object unmarshall(final Object value) {

            return UseMap.create(value,false, (k, v) -> this.value.unmarshall(v));
        }

        @Override
        public Object marshall(final Object value) {

            return Coercion.toMap(value, erasedType, Objects::toString, this.value::marshall);
        }

        @Override
        public Class<?> erasedType() {

            return erasedType;
        }

        @Override
        public Set<Class<?>> dependencies() {

            return value.dependencies();
        }
    }

    @RequiredArgsConstructor
    class OfOptional implements TypeMapper {

        private final Class<?> erasedType;

        @Getter
        private final TypeMapper value;

        @Override
        public Use<?> use() {

            return new UseOptional<>(value.use());
        }

        @Override
        public Object unmarshall(final Object value) {

            if(Optional.class.equals(erasedType)) {
                return this.value.unmarshall(((Optional<?>)value).orElse(null));
            } else if(value == null) {
                return null;
            } else {
                return this.value.unmarshall(value);
            }
        }

        @Override
        public Object marshall(final Object value) {

            if(Optional.class.equals(erasedType)) {
                if(value == null) {
                    return Optional.empty();
                } else {
                    return Optional.ofNullable(this.value.marshall(value));
                }
            } else if(value == null) {
                return null;
            } else {
                return this.value.marshall(value);
            }
        }

        @Override
        public Class<?> erasedType() {

            return erasedType;
        }

        @Override
        public Set<Class<?>> dependencies() {

            return value.dependencies();
        }
    }

    @RequiredArgsConstructor
    @SuppressWarnings("Guava")
    class OfCustom implements TypeMapper {

        private final Class<?> erasedType;

        private final Supplier<Name> qualifiedName;

        private final Supplier<SchemaMapper<?, ?>> mapper;

        public OfCustom(final MappingContext mappingContext, final Class<?> erasedType) {

            this.erasedType = erasedType;
            this.qualifiedName = Suppliers.memoize((SerializableSupplier<Name>) (() -> mappingContext.schemaName(erasedType)));
            this.mapper = Suppliers.memoize((SerializableSupplier<SchemaMapper<?, ?>>) (() -> mappingContext.schemaMapper(erasedType)));
        }

        // FIXME: move out
        private interface SerializableSupplier<T> extends com.google.common.base.Supplier<T>, Serializable {

        }

        public SchemaMapper<?, ?> getMapper() {

            return mapper.get();
        }

        public Name getQualifiedName() {

            return qualifiedName.get();
        }

        @Override
        public Use<?> use() {

            return UseNamed.from(mapper.get().name());
        }

        @Override
        @SuppressWarnings("unchecked")
        public Object unmarshall(final Object value) {

            return ((SchemaMapper<Object, ?>) mapper.get()).unmarshall(value);
        }

        @Override
        public Object marshall(final Object value) {

            return mapper.get().marshall(value);
        }

        @Override
        public Class<?> erasedType() {

            return erasedType;
        }

        @Override
        public Set<Class<?>> dependencies() {

            return ImmutableSet.of(erasedType);
        }
    }

    @RequiredArgsConstructor
    class OfDate implements TypeMapper {

        private final Class<?> erasedType;

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
        public Class<?> erasedType() {

            return erasedType;
        }

        @Override
        public Set<Class<?>> dependencies() {

            return Collections.emptySet();
        }
    }

    @RequiredArgsConstructor
    abstract class OfDateTime<T> implements TypeMapper {

        @Override
        public Use<?> use() {

            return UseDateTime.DEFAULT;
        }

        @Override
        public abstract Class<T> erasedType();

        public abstract T marshallInstant(final Instant value);

        public abstract Instant unmarshallInstant(final T value);

        @Override
        @SuppressWarnings("unchecked")
        public Object unmarshall(final Object value) {

            return unmarshallInstant((T)value);
        }

        @Override
        public Object marshall(final Object value) {

            return marshallInstant(Coercion.toDateTime(value));
        }

        @Override
        public Set<Class<?>> dependencies() {

            return Collections.emptySet();
        }
    }

    @RequiredArgsConstructor
    class OfInstant extends OfDateTime<Instant> {

        @Override
        public Class<Instant> erasedType() {

            return Instant.class;
        }

        @Override
        public Instant marshallInstant(final Instant value) {

            return value;
        }

        @Override
        public Instant unmarshallInstant(final Instant value) {

            return value;
        }
    }

    @RequiredArgsConstructor
    class OfLocalDateTime extends OfDateTime<LocalDateTime> {

        @Override
        public Class<LocalDateTime> erasedType() {

            return LocalDateTime.class;
        }

        @Override
        public LocalDateTime marshallInstant(final Instant value) {

            return LocalDateTime.ofInstant(value, ZoneOffset.UTC.normalized());
        }

        @Override
        public Instant unmarshallInstant(final LocalDateTime value) {

            return value.toInstant(ZoneOffset.UTC);
        }
    }

    @RequiredArgsConstructor
    class OfBinary implements TypeMapper {

        private final Class<?> erasedType;

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
        public Class<?> erasedType() {

            return erasedType;
        }

        @Override
        public Set<Class<?>> dependencies() {

            return Collections.emptySet();
        }
    }

    @RequiredArgsConstructor
    class OfAny implements TypeMapper {

        @Override
        public Use<?> use() {

            return UseAny.DEFAULT;
        }

        @Override
        public Object unmarshall(final Object value) {

            return value;
        }

        @Override
        public Object marshall(final Object value) {

            return value;
        }

        @Override
        public Class<?> erasedType() {

            return Object.class;
        }

        @Override
        public Set<Class<?>> dependencies() {

            return Collections.emptySet();
        }
    }

    @RequiredArgsConstructor
    class OfStringConverted<T> implements TypeMapper {

        private final Class<T> erasedType;

        private final Function<String, T> fromString;

        private final Function<T, String> toString;

        public OfStringConverted(final Class<T> erasedType, final Function<String, T> fromString) {

            this(erasedType, fromString, Object::toString);
        }

        @Override
        public Use<?> use() {

            return UseString.DEFAULT;
        }

        @Override
        @SuppressWarnings("unchecked")
        public Object unmarshall(final Object value) {

            return toString.apply((T)value);
        }

        @Override
        public T marshall(final Object value) {

            return fromString.apply((String)value);
        }

        @Override
        public Class<?> erasedType() {

            return erasedType;
        }

        @Override
        public Set<Class<?>> dependencies() {

            return Collections.emptySet();
        }
    }

    @RequiredArgsConstructor
    @Deprecated
    class OfUse implements TypeMapper {

        @Override
        public Use<?> use() {

            return UseAny.DEFAULT;
        }

        @Override
        public Object unmarshall(final Object value) {

            return ((Use<?>)value).toConfig();
        }

        @Override
        public Use<?> marshall(final Object value) {

            return Use.fromConfig(value);
        }

        @Override
        public Class<?> erasedType() {

            return Use.class;
        }

        @Override
        public Set<Class<?>> dependencies() {

            return Collections.emptySet();
        }
    }
}

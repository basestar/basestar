package io.basestar.storage.elasticsearch.mapping;

/*-
 * #%L
 * basestar-storage-elasticsearch
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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.BaseEncoding;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.*;
import java.util.stream.Collectors;

public interface FieldType {

    BinaryType BINARY = new BinaryType();

    BooleanType BOOLEAN = new BooleanType();

    DateType DATE = new DateType();

    NumericType LONG = new NumericType(NumericType.Type.LONG);

    NumericType DOUBLE = new NumericType(NumericType.Type.DOUBLE);

    TextType TEXT = new TextType();

    KeywordType KEYWORD = new KeywordType();

    Object source();

    Object toSource(Object value);

    Object fromSource(Object value);

    class BinaryType implements FieldType {

        @Override
        public Object source() {

            return ImmutableMap.of("type", "binary");
        }

        @Override
        public String toSource(final Object value) {

            if(value == null) {
                return null;
            } else if(value instanceof byte[]) {
                return BaseEncoding.base64().encode((byte[])value);
            } else {
                throw new IllegalStateException();
            }
        }

        @Override
        public byte[] fromSource(final Object value) {

            if(value == null) {
                return null;
            } else if(value instanceof String) {
                return BaseEncoding.base64().decode((String)value);
            } else {
                throw new IllegalStateException();
            }
        }
    }

    class BooleanType implements FieldType {

        @Override
        public Object source() {

            return ImmutableMap.of("type", "boolean");
        }

        @Override
        public Object toSource(final Object value) {

            return value;
        }

        @Override
        public Object fromSource(final Object value) {

            return value;
        }
    }

    class DateType implements FieldType {

        @Override
        public Object source() {

            return ImmutableMap.of("type", "date");
        }

        @Override
        public Object toSource(final Object value) {

            return value;
        }

        @Override
        public Object fromSource(final Object value) {

            return value;
        }
    }

    class KeywordType implements FieldType {

        @Override
        public Object source() {

            return ImmutableMap.of("type", "keyword");
        }

        @Override
        public Object toSource(final Object value) {

            return value;
        }

        @Override
        public Object fromSource(final Object value) {

            return value;
        }
    }

    @RequiredArgsConstructor
    class NumericType implements FieldType {

        @Getter
        @RequiredArgsConstructor
        public enum Type {
            LONG("long"),
            INTEGER("integer"),
            SHORT("short"),
            BYTE("byte"),
            DOUBLE("double"),
            FLOAT("float"),
            HALF_FLOAT("half_float"),
            SCALED_FLOAT("scaled_float");

            private final String type;
        }

        private final Type type;

        @Override
        public Object source() {

            return ImmutableMap.of("type", type.getType());
        }

        @Override
        public Object toSource(final Object value) {

            return value;
        }

        @Override
        public Object fromSource(final Object value) {

            return value;
        }
    }

    @RequiredArgsConstructor
    class NestedType implements FieldType {

        private final Map<String, FieldType> properties;

        @Override
        public Object source() {

            return ImmutableMap.of("type", "nested", "properties", properties.entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().source())));
        }

        @Override
        public Map<String, Object> toSource(final Object value) {

            if(value == null) {
                return null;
            } else {
                final Map<?, ?> tmp = (Map<?, ?>) value;
                final Map<String, Object> results = new HashMap<>();
                properties.forEach((name, prop) -> {
                    final Object v = tmp.get(name);
                    results.put(name, prop.toSource(v));
                });
                return results;
            }
        }

        @Override
        public Object fromSource(final Object value) {

            if(value == null) {
                return null;
            } else {
                final Map<?, ?> tmp = (Map<?, ?>) value;
                final Map<String, Object> results = new HashMap<>();
                properties.forEach((name, prop) -> {
                    final Object v = tmp.get(name);
                    results.put(name, prop.fromSource(v));
                });
                return results;
            }
        }
    }

    @RequiredArgsConstructor
    class ArrayType implements FieldType {

        private final FieldType valueType;

        @Override
        public Object source() {

            return valueType.source();
        }

        @Override
        public List<?> toSource(final Object value) {

            if(value == null) {
                return null;
            } else {
                return ((Collection<?>) value).stream()
                        .map(valueType::toSource)
                        .collect(Collectors.toList());
            }
        }

        @Override
        public List<?> fromSource(final Object value) {

            if(value == null) {
                return null;
            } else {
                final Collection<?> tmp;
                if (value instanceof Collection) {
                    tmp = (Collection<?>) value;
                } else {
                    tmp = ImmutableList.of(value);
                }
                return tmp.stream()
                        .map(valueType::fromSource)
                        .collect(Collectors.toList());
            }
        }
    }

    @RequiredArgsConstructor
    class MapType implements FieldType {

        private static final String KEY = "key";

        private static final String VALUE = "value";

        private final FieldType valueType;

        @Override
        public Object source() {

            return ImmutableMap.of("type", "nested", "properties", ImmutableMap.of(
                    KEY, KEYWORD.source(),
                    VALUE, valueType.source()
            ));
        }

        @Override
        public Collection<?> toSource(final Object value) {

            if(value == null) {
                return null;
            } else if(value instanceof Map) {
                final List<Object> result = new ArrayList<>();
                ((Map<?, ?>)value).forEach((k, v) -> result.add(ImmutableMap.of(
                        KEY, k,
                        VALUE, valueType.toSource(v)
                )));
                return result;
            } else {
                throw new IllegalStateException();
            }
        }

        @Override
        public Map<String, Object> fromSource(final Object value) {

            if(value == null) {
                return null;
            } else if(value instanceof Collection) {
                final Map<String, Object> result = new HashMap<>();
                ((Collection<?>)value).forEach(v -> {
                    final Map<?, ?> tmp = (Map<?, ?>)v;
                    result.put((String)tmp.get(KEY), valueType.fromSource(tmp.get(VALUE)));
                });
                return result;
            } else {
                throw new IllegalStateException();
            }
        }
    }

    class TextType implements FieldType {

        @Override
        public Object source() {

            return ImmutableMap.of("type", "text");
        }

        @Override
        public Object toSource(final Object value) {

            return value;
        }

        @Override
        public Object fromSource(final Object value) {

            return value;
        }
    }
}

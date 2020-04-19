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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.collect.Multimap;
import io.basestar.expression.Context;
import io.basestar.schema.Expander;
import io.basestar.schema.Instance;
import io.basestar.schema.Schema;
import io.basestar.schema.exception.InvalidTypeException;
import io.basestar.util.Path;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Type Use
 *
 * @param <T>
 */

//@JsonDeserialize(using = TypeUse.Deserializer.class)
public interface Use<T> extends Serializable {

    enum Code {

        NULL,
        BOOLEAN,
        INTEGER,
        NUMBER,
        STRING,
        ENUM,
        ARRAY,
        SET,
        MAP,
        REF,
        STRUCT,
        BINARY
    }

    <R> R visit(Visitor<R> visitor);

    default <T2> T2 cast(final Object o, final Class<T2> as) {

        return as.cast(o);
    }

    Use<?> resolve(Schema.Resolver resolver);

    T create(Object value, boolean expand);

    default T create(Object value) {

        return create(value, false);
    }

    Code code();

    Use<?> typeOf(Path path);

    T expand(T value, Expander expander, Set<Path> expand);

    Set<Path> requiredExpand(Set<Path> paths);

    @Deprecated
    Multimap<Path, Instance> refs(T value);

    @JsonValue
    Object toJson();

    String toString();

    void serializeValue(T value, DataOutput out) throws IOException;

    T deserializeValue(DataInput in) throws IOException;

    T applyVisibility(Context context, T value);

    T evaluateTransients(Context context, T value, Set<Path> expand);

    Set<Path> transientExpand(Path path, Set<Path> expand);

    @JsonCreator
    @SuppressWarnings("unchecked")
    static Use<?> fromConfig(final Object value) {

        final String type;
        final Object config;
        if(value instanceof String) {
            type = (String)value;
            config = Collections.emptyMap();
        } else if(value instanceof Map) {
            final Map.Entry<String, Object> entry = ((Map<String, Object>) value).entrySet().iterator().next();
            type = entry.getKey();
            config = entry.getValue();
        } else {
            throw new InvalidTypeException();
        }
        switch(type) {
            case UseBoolean.NAME:
                return UseBoolean.from(config);
            case UseInteger.NAME:
                return UseInteger.from(config);
            case UseNumber.NAME:
                return UseNumber.from(config);
            case UseString.NAME:
                return UseString.from(config);
            case UseArray.NAME:
                return UseArray.from(config);
            case UseSet.NAME:
                return UseSet.from(config);
            case UseMap.NAME:
                return UseMap.from(config);
            case UseBinary.NAME:
                return UseBinary.from(config);
            default:
                return UseNamed.from(type, config);
        }
    }

    default void serialize(final T value, final DataOutput out) throws IOException {

        if(value == null) {
            out.writeByte(Code.NULL.ordinal());
        } else {
            out.writeByte(code().ordinal());
            serializeValue(value, out);
        }
    }

    default T deseralize(DataInput in) throws IOException {

        final byte ordinal = in.readByte();
        final Code code = Code.values()[ordinal];
        switch(code) {
            case NULL:
                return null;
            default:
                if(code == code()) {
                    return deserializeValue(in);
                } else {
                    throw new IOException("Expected code " + code() + " but got " + code);
                }

        }
    }

    @SuppressWarnings("unchecked")
    static <T> T deserializeAny(final DataInput in) throws IOException {

        final byte ordinal = in.readByte();
        final Code code = Code.values()[ordinal];
        switch(code) {
            case NULL:
                return null;
            case BOOLEAN:
                return (T)UseBoolean.DEFAULT.deserializeValue(in);
            case INTEGER:
                return (T)UseInteger.DEFAULT.deserializeValue(in);
            case NUMBER:
                return (T)UseNumber.DEFAULT.deserializeValue(in);
            case STRING:
                return (T)UseString.DEFAULT.deserializeValue(in);
            case ENUM:
                return (T)UseEnum.deserializeAnyValue(in);
            case ARRAY:
                return (T)UseArray.deserializeAnyValue(in);
            case SET:
                return (T)UseSet.deserializeAnyValue(in);
            case MAP:
                return (T)UseMap.deserializeAnyValue(in);
            case REF:
                return (T)UseRef.deserializeAnyValue(in);
            case STRUCT:
                return (T)UseStruct.deserializeAnyValue(in);
            case BINARY:
                return (T)UseBinary.DEFAULT.deserializeValue(in);
            default:
                throw new IllegalStateException();
        }
    }

    interface Visitor<R> {

        R visitBoolean(UseBoolean type);

        R visitInteger(UseInteger type);

        R visitNumber(UseNumber type);

        R visitString(UseString type);

        R visitEnum(UseEnum type);

        R visitRef(UseRef type);

        <T> R visitArray(UseArray<T> type);

        <T> R visitSet(UseSet<T> type);

        <T> R visitMap(UseMap<T> type);

        R visitStruct(UseStruct type);

        R visitBinary(UseBinary type);

        interface Defaulting<R> extends Visitor<R> {

            R visitDefault(Use<?> type);

            default R visitScalar(final UseScalar<?> type) {

                return visitDefault(type);
            }

            default <T> R visitCollection(final UseCollection<T, ? extends Collection<T>> type) {

                return visitDefault(type);
            }

            @Override
            default R visitBoolean(final UseBoolean type) {

                return visitScalar(type);
            }

            @Override
            default R visitInteger(final UseInteger type) {

                return visitScalar(type);
            }

            @Override
            default R visitNumber(final UseNumber type) {

                return visitScalar(type);
            }

            @Override
            default R visitString(final UseString type) {

                return visitScalar(type);
            }

            @Override
            default R visitEnum(final UseEnum type) {

                return visitScalar(type);
            }

            @Override
            default R visitRef(final UseRef type) {

                return visitDefault(type);
            }

            @Override
            default <T> R visitArray(final UseArray<T> type) {

                return visitCollection(type);
            }

            @Override
            default <T> R visitSet(final UseSet<T> type) {

                return visitCollection(type);
            }

            @Override
            default <T> R visitMap(final UseMap<T> type) {

                return visitDefault(type);
            }

            @Override
            default R visitStruct(final UseStruct type) {

                return visitDefault(type);
            }

            @Override
            default R visitBinary(final UseBinary type) {

                return visitScalar(type);
            }
        }
    }
}

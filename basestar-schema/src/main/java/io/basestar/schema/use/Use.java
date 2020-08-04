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
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.schema.Constraint;
import io.basestar.schema.Schema;
import io.basestar.schema.exception.TypeSyntaxException;
import io.basestar.schema.util.Expander;
import io.basestar.schema.util.Ref;
import io.basestar.util.Name;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

/**
 * Type Use
 *
 * @param <T>
 */

//@JsonDeserialize(using = TypeUse.Deserializer.class)
public interface Use<T> extends Serializable {

    enum Code {

        NULL,
        ANY,
        BOOLEAN,
        INTEGER,
        NUMBER,
        STRING,
        ENUM,
        ARRAY,
        SET,
        MAP,
        OBJECT,
        STRUCT,
        BINARY,
        DATE,
        DATETIME,
        VIEW,
        OPTIONAL
    }

    <R> R visit(Visitor<R> visitor);

    default <T2> T2 cast(final Object o, final Class<T2> as) {

        return as.cast(o);
    }

    Use<?> resolve(Schema.Resolver resolver);

    T create(Object value, Set<Name> expand, boolean suppress);

    default T create(final Object value) {

        return create(value, null, false);
    }

    Code code();

    Use<?> typeOf(Name name);

    T expand(T value, Expander expander, Set<Name> expand);

    Set<Name> requiredExpand(Set<Name> names);

    @JsonValue
    Object toConfig();

    String toString();

    void serializeValue(T value, DataOutput out) throws IOException;

    T deserializeValue(DataInput in) throws IOException;

    T applyVisibility(Context context, T value);

    T evaluateTransients(Context context, T value, Set<Name> expand);

    Set<Name> transientExpand(Name name, Set<Name> expand);

    Set<Constraint.Violation> validate(Context context, Name name, T value);

    io.swagger.v3.oas.models.media.Schema<?> openApi();

    Set<Expression> refQueries(Name otherSchemaName, Set<Name> expand, Name name);

    Set<Name> refExpand(Name otherSchemaName, Set<Name> expand);

    Map<Ref, Long> refVersions(T value);

    void collectDependencies(Set<Name> expand, Map<Name, Schema<?>> out);

    default boolean isOptional() {

        return false;
    }

    default Use<T> optional(final boolean optional) {

        // Inverse implemented in UseOptional
        if(optional) {
            return new UseOptional<>(this);
        } else {
            return this;
        }
    }

    @JsonCreator
    @SuppressWarnings("unchecked")
    static Use<?> fromConfig(final Object value) {

        final String typeName;
        final Object config;
        if (value instanceof String) {
            typeName = ((String) value).trim();
            config = Collections.emptyMap();
        } else if (value instanceof Map) {
            final Map.Entry<String, Object> entry = ((Map<String, Object>) value).entrySet().iterator().next();
            typeName = entry.getKey().trim();
            config = entry.getValue();
        } else {
            throw new TypeSyntaxException();
        }
        final String type;
        final boolean optional;
        if(typeName.endsWith(UseOptional.SYMBOL)) {
            type = typeName.substring(0, typeName.length() - UseOptional.SYMBOL.length()).trim();
            optional = true;
        } else {
            type = typeName;
            optional = false;
        }
        final Use<?> result = fromConfig(type, config);
        if(optional) {
            return result.optional(true);
        } else {
            return result;
        }
    }

    static Use<?> fromConfig(final String type, final Object config) {

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
            case UseDate.NAME:
                return UseDate.from(config);
            case UseDateTime.NAME:
                return UseDateTime.from(config);
            case UseOptional.NAME:
                return UseOptional.from(config);
            default:
                return UseNamed.from(type, config);
        }
    }

    @SuppressWarnings("unchecked")
    static <T extends Use<?>, V extends Use<?>> T fromNestedConfig(final Object config, final BiFunction<V, Map<String, Object>, T> apply) {

        final Use<?> nestedType;
        final Map<String, Object> nestedConfig;
        if(config instanceof String) {
            nestedType = Use.fromConfig(config);
            nestedConfig = null;
        } else if(config instanceof Map) {
            final Map<String, Object> map = (Map<String, Object>) config;
            if(map.containsKey("type")) {
                nestedType = Use.fromConfig(map.get("type"));
                nestedConfig = map;
            } else {
                nestedType = Use.fromConfig(map);
                nestedConfig = null;
            }
        } else {
            throw new TypeSyntaxException();
        }
        return apply.apply((V)nestedType, nestedConfig);
    }

    default void serialize(final T value, final DataOutput out) throws IOException {

        if(value == null) {
            out.writeByte(Code.NULL.ordinal());
        } else {
            out.writeByte(code().ordinal());
            serializeValue(value, out);
        }
    }

    default T deserialize(final DataInput in) throws IOException {

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
            case OBJECT:
                return (T) UseObject.deserializeAnyValue(in);
            case STRUCT:
                return (T)UseStruct.deserializeAnyValue(in);
            case BINARY:
                return (T)UseBinary.DEFAULT.deserializeValue(in);
            case DATE:
                return (T)UseDate.DEFAULT.deserializeValue(in);
            case DATETIME:
                return (T)UseDateTime.DEFAULT.deserializeValue(in);
            case VIEW:
                return (T) UseView.deserializeAnyValue(in);
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

        R visitObject(UseObject type);

        <T> R visitArray(UseArray<T> type);

        <T> R visitSet(UseSet<T> type);

        <T> R visitMap(UseMap<T> type);

        R visitStruct(UseStruct type);

        R visitBinary(UseBinary type);

        R visitDate(UseDate type);

        R visitDateTime(UseDateTime type);

        R visitView(UseView type);

        default R visitAny(UseAny useAny) {

            // Until properly implemented
            throw new UnsupportedOperationException();
        }

        <T> R visitOptional(UseOptional<T> type);

        interface Defaulting<R> extends Visitor<R> {

            R visitDefault(Use<?> type);

            default R visitScalar(final UseScalar<?> type) {

                return visitDefault(type);
            }

            default <T> R visitCollection(final UseCollection<T, ? extends Collection<T>> type) {

                return visitDefault(type);
            }

            default R visitInstance(final UseInstance type) {

                return visitDefault(type);
            }

            default R visitLinkable(final UseLinkable type) {

                return visitInstance(type);
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
            default R visitObject(final UseObject type) {

                return visitLinkable(type);
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

                return visitInstance(type);
            }

            @Override
            default R visitBinary(final UseBinary type) {

                return visitScalar(type);
            }

            @Override
            default R visitDate(final UseDate type) {

                return visitScalar(type);
            }

            @Override
            default R visitDateTime(final UseDateTime type) {

                return visitScalar(type);
            }

            @Override
            default R visitView(final UseView type) {

                return visitLinkable(type);
            }

            @Override
            default <T> R visitOptional(final UseOptional<T> type) {

                // Least astonishment is to unwrap so that handling is optional
                return type.getType().visit(this);
            }
        }
    }
}

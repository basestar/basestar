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
import io.basestar.expression.type.Numbers;
import io.basestar.schema.Bucketing;
import io.basestar.schema.Constraint;
import io.basestar.schema.LinkableSchema;
import io.basestar.schema.Schema;
import io.basestar.schema.exception.InvalidKeyException;
import io.basestar.schema.exception.TypeSyntaxException;
import io.basestar.schema.util.Cascade;
import io.basestar.schema.util.Expander;
import io.basestar.schema.util.Ref;
import io.basestar.schema.util.ValueContext;
import io.basestar.secret.Secret;
import io.basestar.util.Name;
import io.basestar.util.Nullsafe;
import io.basestar.util.Page;
import io.basestar.util.Warnings;
import io.leangen.geantyref.GenericTypeReflector;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.time.Instant;
import java.time.LocalDate;
import java.util.*;
import java.util.function.BiFunction;

/**
 * Type Use
 *
 * @param <T>
 */

@SuppressWarnings(Warnings.RETURN_GENERIC_WILDCARD)
public interface Use<T> extends Serializable {

    String OPTIONAL_SYMBOL = "?";

    enum Code {

        NULL,
        ANY,
        BOOLEAN,
        INTEGER,
        NUMBER,
        DECIMAL,
        STRING,
        ENUM,
        ARRAY,
        SET,
        MAP,
        REF,
        STRUCT,
        BINARY,
        DATE,
        DATETIME,
        VIEW,
        SECRET,
        PAGE,
        COMPOSITE;
    }

    <R> R visit(Visitor<R> visitor);

    default T cast(final Object o, final Class<T> as) {

        return as.cast(o);
    }

    default boolean isNumeric() {

        return false;
    }

    default boolean isStringLike() {

        return true;
    }

    Use<?> resolve(Schema.Resolver resolver);

    T create(ValueContext context, Object value, Set<Name> expand);

    default T create(final Object value, final Set<Name> expand, final boolean suppress) {

        return create(ValueContext.standardOrSuppressing(suppress), value, expand);
    }

    default T create(final Object value, final Set<Name> expand) {

        return create(value, expand, true);
    }

    default T create(final Object value) {

        return create(value, null);
    }

    Code code();

    default Use<?> typeOf(final Name name) {

        return optionalTypeOf(name).orElse(UseAny.DEFAULT);
    }

    Optional<Use<?>> optionalTypeOf(Name name);

    default Type javaType() {

        return javaType(Name.empty());
    }

    Type javaType(Name name);

    T expand(Name parent, T value, Expander expander, Set<Name> expand);

    void expand(Name parent, Expander expander, Set<Name> expand);

    Set<Name> requiredExpand(Set<Name> names);

    T defaultValue();

    @JsonValue
    default Object toConfig() {

        return toConfig(false);
    }

    Object toConfig(boolean optional);

    String toString();

    String toString(T value);

    void serializeValue(T value, DataOutput out) throws IOException;

    T deserializeValue(DataInput in) throws IOException;

    T applyVisibility(Context context, T value);

    T evaluateTransients(Context context, T value, Set<Name> expand);

    Set<Name> transientExpand(Name name, Set<Name> expand);

    Set<Constraint.Violation> validate(Context context, Name name, T value);

    io.swagger.v3.oas.models.media.Schema<?> openApi(Set<Name> expand);

    Set<Expression> refQueries(Name otherSchemaName, Set<Name> expand, Name name);

    Set<Expression> cascadeQueries(Cascade cascade, Name otherSchemaName, Name name);

    Set<Name> refExpand(Name otherSchemaName, Set<Name> expand);

    Map<Ref, Long> refVersions(T value);

    void collectDependencies(Set<Name> expand, Map<Name, Schema<?>> out);

    void collectMaterializationDependencies(Set<Name> expand, Map<Name, LinkableSchema> out);

    boolean isCompatibleBucketing(List<Bucketing> other, Name name);

    default Object[] key(final T value) {

        throw new InvalidKeyException(this);
    }

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

    static String name(final String name, final boolean optional) {

        return optional ? (name + OPTIONAL_SYMBOL) : name;
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
        if(typeName.endsWith(OPTIONAL_SYMBOL)) {
            type = typeName.substring(0, typeName.length() - OPTIONAL_SYMBOL.length()).trim();
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

    static Use<?> fromJavaType(final Type type) {

        final Class<?> erased = GenericTypeReflector.erase(type);
        if(Numbers.isBooleanType(erased)) {
            return UseBoolean.DEFAULT;
        } else if(Numbers.isDecimalType(erased)) {
            return UseDecimal.DEFAULT;
        } else if(Numbers.isIntegerType(erased)) {
            return UseInteger.DEFAULT;
        } else if(Numbers.isNumberType(erased)) {
            return UseNumber.DEFAULT;
        } else if(String.class.isAssignableFrom(erased)) {
            return UseString.DEFAULT;
        } else if(LocalDate.class.isAssignableFrom(erased)) {
            return UseDate.DEFAULT;
        } else if(Instant.class.isAssignableFrom(erased)) {
            return UseDateTime.DEFAULT;
        } else if(Page.class.isAssignableFrom(erased)) {
            final TypeVariable<? extends Class<?>> var = Page.class.getTypeParameters()[0];
            final Type arg = Nullsafe.orDefault(GenericTypeReflector.getTypeParameter(type, var), Object.class);
            return UsePage.from(arg);
        } else if(List.class.isAssignableFrom(erased)) {
            final TypeVariable<? extends Class<?>> var = List.class.getTypeParameters()[0];
            final Type arg = Nullsafe.orDefault(GenericTypeReflector.getTypeParameter(type, var), Object.class);
            return UseArray.from(arg);
        } else if(Set.class.isAssignableFrom(erased)) {
            final TypeVariable<? extends Class<?>> var = Set.class.getTypeParameters()[0];
            final Type arg = Nullsafe.orDefault(GenericTypeReflector.getTypeParameter(type, var), Object.class);
            return UseSet.from(arg);
        } else if(Map.class.isAssignableFrom(erased)) {
            final TypeVariable<? extends Class<?>> var = Map.class.getTypeParameters()[1];
            final Type arg = Nullsafe.orDefault(GenericTypeReflector.getTypeParameter(type, var), Object.class);
            return UseMap.from(arg);
        } else if(Secret.class.isAssignableFrom(erased)) {
            return UseSecret.DEFAULT;
        } else if(byte[].class.isAssignableFrom(erased)) {
            return UseBinary.DEFAULT;
        } else {
            return UseAny.DEFAULT;
        }
    }

    static Use<?> fromConfig(final String type, final Object config) {

        switch(type) {
            case UseAny.NAME:
                return UseAny.from(config);
            case UseBoolean.NAME:
                return UseBoolean.from(config);
            case UseInteger.NAME:
                return UseInteger.from(config);
            case UseNumber.NAME:
                return UseNumber.from(config);
            case UseDecimal.NAME:
                return UseDecimal.from(config);
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
            case UseEnum.NAME:
                return UseEnum.from(config);
            case UseComposite.NAME:
                return UseComposite.from(config);
            case UseSecret.NAME:
                return UseSecret.from(config);
            default:
                return UseNamed.from(type, config);
        }
    }

    @SuppressWarnings("unchecked")
    static <T extends Use<?>, V extends Use<?>> T fromNestedConfig(final Object config, final BiFunction<V, Map<String, Object>, T> apply) {

        final Use<?> nestedType;
        final Map<String, Object> nestedConfig;
        if(config instanceof Use<?>) {
            nestedType = (Use<?>)config;
            nestedConfig = null;
        } else if(config instanceof Type) {
            nestedType = Use.fromJavaType((Type)config);
            nestedConfig = null;
        } else if(config instanceof String) {
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
            case DECIMAL:
                return (T)UseDecimal.DEFAULT.deserializeValue(in);
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
                return (T) UseRef.deserializeAnyValue(in);
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
            case COMPOSITE:
                return (T)UseComposite.deserializeAnyValue(in);
            default:
                throw new IllegalStateException();
        }
    }

    interface Visitor<R> {

        default <T> R visit(Use<T> type) {

            return type.visit(this);
        }

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

        R visitDate(UseDate type);

        R visitDateTime(UseDateTime type);

        R visitView(UseView type);

        <T> R visitOptional(UseOptional<T> type);

        R visitAny(UseAny type);

        R visitSecret(UseSecret type);

        <T> R visitPage(UsePage<T> type);

        R visitDecimal(UseDecimal type);

        R visitComposite(UseComposite type);

//        default R visitCallable(UseCallable useCallable) {
//
//            throw new UnsupportedOperationException();
//        }

        interface Defaulting<R> extends Visitor<R> {

            default <T> R visitDefault(final Use<T> type) {

                throw new UnsupportedOperationException("Type " + type.code() + " not supported");
            }

            default <T> R visitScalar(final UseScalar<T> type) {

                return visitDefault(type);
            }

            default <T> R visitStringLike(final UseStringLike<T> type) {

                return visitScalar(type);
            }

            default <T extends Number> R visitNumeric(final UseNumeric<T> type) {

                return visitScalar(type);
            }

            default <V, T> R visitContainer(final UseContainer<V, T> type) {

                return visitDefault(type);
            }

            default <V, T extends Collection<V>> R visitCollection(final UseCollection<V, T> type) {

                return visitContainer(type);
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

                return visitNumeric(type);
            }

            @Override
            default R visitNumber(final UseNumber type) {

                return visitNumeric(type);
            }

            @Override
            default R visitDecimal(final UseDecimal type) {

                return visitNumeric(type);
            }

            @Override
            default R visitString(final UseString type) {

                return visitStringLike(type);
            }

            @Override
            default R visitEnum(final UseEnum type) {

                return visitStringLike(type);
            }

            @Override
            default R visitRef(final UseRef type) {

                return visitLinkable(type);
            }

            @Override
            default <T> R visitArray(final UseArray<T> type) {

                return visitCollection(type);
            }

            @Override
            default <T> R visitPage(final UsePage<T> type) {

                return visitCollection(type);
            }

            @Override
            default <T> R visitSet(final UseSet<T> type) {

                return visitCollection(type);
            }

            @Override
            default <T> R visitMap(final UseMap<T> type) {

                return visitContainer(type);
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

                return visitStringLike(type);
            }

            @Override
            default R visitDateTime(final UseDateTime type) {

                return visitStringLike(type);
            }

            @Override
            default R visitView(final UseView type) {

                return visitLinkable(type);
            }

            @Override
            default R visitAny(final UseAny type) {

                return visitDefault(type);
            }

            @Override
            default <T> R visitOptional(final UseOptional<T> type) {

                // Least astonishment
                return visit(type.getType());
            }

            @Override
            default R visitSecret(final UseSecret type) {

                return visitScalar(type);
            }

            @Override
            default R visitComposite(final UseComposite type) {

                return visitDefault(type);
            }
        }
    }

    boolean areEqual(T a, T b);

    static Use<?> commonBase(final Use<?> a, final Use<?> b) {

        if(a instanceof UseBoolean && b instanceof UseBoolean) {
            return UseBoolean.DEFAULT;
        } else if(a instanceof UseNumeric && b instanceof UseNumeric) {
            if(a instanceof UseDecimal || b instanceof UseDecimal) {
                return UseDecimal.DEFAULT;
            } else if(a instanceof UseNumber || b instanceof UseNumber) {
                return UseNumber.DEFAULT;
            } else {
                return UseInteger.DEFAULT;
            }
        } else if(a instanceof UseString && b instanceof UseString) {
            return UseString.DEFAULT;
        } else if(a instanceof UseArray && b instanceof UseArray) {
            return UseArray.from(commonBase(((UseArray<?>) a).getType(), ((UseArray<?>) b).getType()));
        } else if(a instanceof UseSet && b instanceof UseSet) {
            return UseSet.from(commonBase(((UseSet<?>) a).getType(), ((UseSet<?>) b).getType()));
        } else if(a instanceof UseMap && b instanceof UseMap) {
            return UseMap.from(commonBase(((UseMap<?>) a).getType(), ((UseMap<?>) b).getType()));
        } else if(a instanceof UseEnum && b instanceof UseEnum) {
            if(((UseEnum) a).getName().equals(((UseEnum) b).getName())) {
                return a;
            }
        } else if(a instanceof UseStruct && b instanceof UseStruct) {
            if(((UseStruct) a).getName().equals(((UseStruct) b).getName())) {
                return a;
            }
        } else if(a instanceof UseRef && b instanceof UseRef) {
            if(((UseRef) a).getName().equals(((UseRef) b).getName())) {
                return a;
            }
        } else if(a instanceof UseView && b instanceof UseView) {
            if(((UseView) a).getName().equals(((UseView) b).getName())) {
                return a;
            }
        } else if((a instanceof UseDate || a instanceof UseDateTime) && (b instanceof UseDate || b instanceof UseDateTime)) {
            if(a instanceof UseDateTime || b instanceof UseDateTime) {
                return UseDateTime.DEFAULT;
            } else {
                return UseDate.DEFAULT;
            }
        } else if(a instanceof UseBinary && b instanceof UseBinary) {
            return UseBinary.DEFAULT;
        } else if(a instanceof UseOptional<?> && b instanceof UseOptional<?>) {
            return UseOptional.from(commonBase(((UseOptional<?>) a).getType(), ((UseOptional<?>) b).getType()));
        }

        return UseAny.DEFAULT;
    }

    static Use<?> fromName(final String name) {

        switch (name.toLowerCase()) {
            case UseBoolean.NAME:
                return UseBoolean.DEFAULT;
            case UseInteger.NAME:
                return UseInteger.DEFAULT;
            case UseNumber.NAME:
                return UseNumber.DEFAULT;
            case UseString.NAME:
                return UseString.DEFAULT;
            case UseDate.NAME:
                return UseDate.DEFAULT;
            case UseDateTime.NAME:
                return UseDateTime.DEFAULT;
            case UseDecimal.NAME:
                return UseDecimal.DEFAULT;
            default:
                throw new UnsupportedOperationException("Type " + name + " not recognized");
        }
    }
}

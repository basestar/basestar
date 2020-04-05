package io.basestar.schema.use;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.collect.Multimap;
import io.basestar.schema.Expander;
import io.basestar.schema.Instance;
import io.basestar.schema.Schema;
import io.basestar.schema.exception.InvalidTypeException;
import io.basestar.util.Path;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
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

    <R> R visit(Visitor<R> visitor);

    default <T2> T2 cast(final Object o, final Class<T2> as) {

        return as.cast(o);
    }

    Use<?> resolve(Schema.Resolver resolver);

    T create(Object value);

    Code code();

    Use<?> typeOf(Path path);

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

    void serializeValue(T value, DataOutput out) throws IOException;

    T deserializeValue(DataInput in) throws IOException;

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
                return (T)UseObject.deserializeAnyValue(in);
            case STRUCT:
                return (T)UseStruct.deserializeAnyValue(in);
            case BINARY:
                return (T)UseBinary.DEFAULT.deserializeValue(in);
//            case TUPLE:
//                return (T)UseTuple.deserializeValue(in);
            default:
                throw new IllegalStateException();
        }
    }

    T expand(T value, Expander expander, Set<Path> expand);

//    Map<String,Object> openApiType();

    @Deprecated
    Set<Path> requireExpand(Set<Path> paths);

    @Deprecated
    Multimap<Path, Instance> refs(T value);

    @JsonValue
    Object toJson();

    String toString();

    interface Visitor<R> {

        R visitBoolean(UseBoolean type);

        R visitInteger(UseInteger type);

        R visitNumber(UseNumber type);

        R visitString(UseString type);

        R visitEnum(UseEnum type);

        R visitRef(UseObject type);

        <T> R visitArray(UseArray<T> type);

        <T> R visitSet(UseSet<T> type);

        <T> R visitMap(UseMap<T> type);

        R visitStruct(UseStruct type);

//        R visitTuple(UseTuple type);

        R visitBinary(UseBinary type);
    }

//    class Deserializer extends JsonDeserializer {
//
//        @Override
//        public Object deserialize(final JsonParser jsonParser, final DeserializationContext context) throws IOException, JsonProcessingException {
//
//            if(jsonParser.getCurrentToken() == JsonToken.START_OBJECT) {
//                jsonParser.nextToken();
//            } else {
//
//            }
//            return context.getParser().getParsingContext().pathAsPointer();
//        }
//    }

    //    class Deserializer extends JsonDeserializer {
//
//        @Override
//        public Object deserialize(final JsonParser jsonParser, final DeserializationContext context) throws IOException, JsonProcessingException {
//
//            return context.getParser().getParsingContext().pathAsPointer();
//        }
//    }
}

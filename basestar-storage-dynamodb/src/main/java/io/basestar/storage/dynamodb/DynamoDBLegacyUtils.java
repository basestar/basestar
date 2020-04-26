package io.basestar.storage.dynamodb;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.google.common.collect.ImmutableSet;
import io.basestar.schema.Reserved;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class DynamoDBLegacyUtils {

    public static Map<String, Object> fromItem(final Map<String, AttributeValue> values) {

        final Map<String, Object> result = new HashMap<>();
        values.forEach((k, v) -> result.put(k, fromAttributeValue(v)));
        return result;
    }

    public static Object fromAttributeValue(final AttributeValue value) {

        if(value == null || value.isNULL() != null) {
            return null;
        } else if(value.isBOOL() != null) {
            return value.getBOOL();
        } else if(value.getN() != null) {
            return DynamoDBUtils.parseNumber(value.getN());
        } else if(value.getS() != null) {
            return value.getS();
        } else if(value.getB() != null) {
            return value.getB().array();
        } else if(value.getSS() != null) {
            return ImmutableSet.copyOf(value.getSS());
        } else if(value.getNS() != null) {
            return value.getNS().stream().map(DynamoDBUtils::parseNumber)
                    .collect(Collectors.toSet());
        } else if(value.getBS() != null) {
            return value.getBS().stream().map(ByteBuffer::array)
                    .collect(Collectors.toSet());
        } else if(value.getL() != null) {
            return value.getL().stream().map(DynamoDBLegacyUtils::fromAttributeValue)
                    .collect(Collectors.toList());
        } else if(value.getM() != null) {
            final Map<String, Object> result = new HashMap<>();
            value.getM().forEach((k, v) -> result.put(k, fromAttributeValue(v)));
            return result;
        } else {
            log.error("Got an ambiguous empty item, returning null");
            return null;
        }
    }

    public static Map<String, AttributeValue> toItem(final Map<String, Object> values) {

        return values.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> toAttributeValue(entry.getValue())));
    }

    public static AttributeValue toAttributeValue(final Object value) {

        if(value == null) {
            return new AttributeValue().withNULL(true);
        } else if(value instanceof Boolean) {
            return new AttributeValue().withBOOL((Boolean)value);
        } else if(value instanceof Number) {
            return new AttributeValue().withN(value.toString());
        } else if(value instanceof String) {
            return new AttributeValue().withS(value.toString());
        } else if(value instanceof byte[]) {
            return new AttributeValue().withB(ByteBuffer.wrap((byte[])value));
        } else if(value instanceof Collection) {
            return new AttributeValue().withL(((Collection<?>)value).stream().map(DynamoDBLegacyUtils::toAttributeValue)
                    .collect(Collectors.toList()));
        } else if(value instanceof Map) {
            return new AttributeValue().withM(((Map<?, ?>)value).entrySet().stream()
                    .collect(Collectors.toMap(
                            entry -> entry.getKey().toString(),
                            entry -> toAttributeValue(entry.getValue()))));
        } else {
            throw new IllegalStateException();
        }
    }

    public static String id(final Map<String, AttributeValue> values) {

        return (String)fromAttributeValue(values.get(Reserved.ID));
    }

    public static Long version(final Map<String, AttributeValue> values) {

        return (Long)fromAttributeValue(values.get(Reserved.VERSION));
    }

    public static String schema(final Map<String, AttributeValue> values) {

        return (String)fromAttributeValue(values.get(Reserved.SCHEMA));
    }
}

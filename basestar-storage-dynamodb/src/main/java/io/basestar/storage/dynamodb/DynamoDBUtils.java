package io.basestar.storage.dynamodb;

/*-
 * #%L
 * basestar-storage-dynamodb
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

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableSet;
import io.basestar.schema.ObjectSchema;
import io.basestar.schema.Reserved;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.core.BytesWrapper;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class DynamoDBUtils {

    public static final int MAX_READ_BATCH_SIZE = 100;

    public static final int MAX_ITEM_SIZE = 400_000;

    public static Map<String, AttributeValue> toItem(final Map<String, Object> values) {

        return values.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> DynamoDBUtils.toAttributeValue(entry.getValue())));
    }

    public static AttributeValue toAttributeValue(final Object value) {

        if(value == null) {
            return AttributeValue.builder().nul(true).build();
        } else if(value instanceof Boolean) {
            return AttributeValue.builder().bool((Boolean)value).build();
        } else if(value instanceof Number) {
            return AttributeValue.builder().n(value.toString()).build();
        } else if(value instanceof String) {
            final String str = (String)value;
            if(str.isEmpty()) {
                return AttributeValue.builder().nul(true).build();
            } else {
                return AttributeValue.builder().s(value.toString()).build();
            }
        } else if(value instanceof byte[]) {
            return AttributeValue.builder().b(SdkBytes.fromByteArray((byte[])value)).build();
        } else if(value instanceof Collection) {
            return AttributeValue.builder()
                    .l(((Collection<?>)value).stream().map(DynamoDBUtils::toAttributeValue)
                            .collect(Collectors.toList()))
                    .build();
        } else if(value instanceof Map) {
            return AttributeValue.builder()
                    .m(((Map<?, ?>)value).entrySet().stream()
                            .collect(Collectors.toMap(
                                    entry -> entry.getKey().toString(),
                                    entry -> toAttributeValue(entry.getValue()))))
                    .build();
        } else {
            throw new IllegalStateException();
        }
    }

    public static Map<String, Object> fromItem(final Map<String, AttributeValue> values) {

        final Map<String, Object> result = new HashMap<>();
        values.forEach((k, v) -> result.put(k, fromAttributeValue(v)));
        return result;
    }

    public static Object fromAttributeValue(final AttributeValue value) {

        if(value == null || value.nul() != null) {
            return null;
        } else if(value.bool() != null) {
            return value.bool();
        } else if(value.n() != null) {
            return parseNumber(value.n());
        } else if(value.s() != null) {
            return value.s();
        } else if(value.b() != null){
            return value.b().asByteArray();
        } else if(value.hasL()) {
            return value.l().stream().map(DynamoDBUtils::fromAttributeValue)
                    .collect(Collectors.toList());
        } else if(value.hasM()) {
            final Map<String, Object> result = new HashMap<>();
            value.m().forEach((k, v) -> result.put(k, fromAttributeValue(v)));
            return result;
        } else if(value.hasSs()) {
            return ImmutableSet.copyOf(value.ss());
        } else if(value.hasNs()) {
            return value.ns().stream().map(DynamoDBUtils::parseNumber).collect(Collectors.toSet());
        } else if(value.hasBs()) {
            return value.bs().stream().map(BytesWrapper::asByteArray).collect(Collectors.toSet());
        } else {
            throw new IllegalStateException("Unknown item type: " + value);
        }
    }

    static Number parseNumber(final String str) {

        if(str.contains(".")) {
            return Double.valueOf(str);
        } else {
            return Long.valueOf(str);
        }
    }

    // https://medium.com/@zaccharles/calculating-a-dynamodb-items-size-and-consumed-capacity-d1728942eb7c
    // Generally erring on the side of caution

    public static long itemSize(final Map<String, AttributeValue> values) {

        return values.entrySet().stream()
                .mapToLong(entry -> {
                    final long keySize = stringSize(entry.getKey());
                    return 1L + keySize + attributeValueSize(entry.getValue());
                }).sum();
    }

    private static long stringSize(final String str) {

        return 1L + str.getBytes(Charsets.UTF_8).length;
    }

    public static long attributeValueSize(final AttributeValue value) {

        if(value == null || value.nul() != null) {
            return 1L;
        } else if(value.bool() != null) {
            return 1L;
        } else if(value.n() != null) {
            return 21L;
        } else if(value.s() != null) {
            return stringSize(value.s());
        } else if(value.b() != null){
            return value.b().asByteArray().length + 1L;
        } else if(value.hasL()) {
            return 3L + value.l().stream().mapToLong(v -> 1L + attributeValueSize(v)).sum();
        } else if(value.hasM()) {
            return 3L + itemSize(value.m());
        } else {
            throw new IllegalStateException("Unknown item type: " + value);
        }
    }

    public static CreateTableRequest createTableRequest(final TableDescription table) {

        return createTableRequest(table, BillingMode.PAY_PER_REQUEST, null);
    }

    public static CreateTableRequest createTableRequest(final TableDescription table, final BillingMode billingMode, final ProvisionedThroughput throughput) {

        CreateTableRequest.Builder builder = CreateTableRequest.builder()
                .tableName(table.tableName())
                .keySchema(table.keySchema())
                .attributeDefinitions(table.attributeDefinitions())
                .billingMode(billingMode)
                .provisionedThroughput(throughput);

        if(table.globalSecondaryIndexes() != null) {
            final List<GlobalSecondaryIndex> gsis = new ArrayList<>();
            table.globalSecondaryIndexes().forEach(gsi -> gsis.add(
                    GlobalSecondaryIndex.builder()
                            .indexName(gsi.indexName())
                            .keySchema(gsi.keySchema())
                            .projection(gsi.projection())
                            .provisionedThroughput(throughput)
                            .build()
            ));
            if(!gsis.isEmpty()) {
                builder = builder.globalSecondaryIndexes(gsis);
            }
        }

        return builder.build();
    }

    public static KeySchemaElement keySchemaElement(final String attributeName, final KeyType keyType) {

        return KeySchemaElement.builder().attributeName(attributeName).keyType(keyType).build();
    }

    public static AttributeDefinition attributeDefinition(final String attributeName, final ScalarAttributeType attributeType) {

        return AttributeDefinition.builder().attributeName(attributeName).attributeType(attributeType).build();
    }

    public static Map<String, Object> fromOversizeBytes(final byte[] bytes) {

        try(final ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
            final DataInputStream dis = new DataInputStream(bais)) {
            return ObjectSchema.deserialize(dis);
        } catch (final IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public static byte[] toOversizeBytes(final ObjectSchema schema, final Map<String, Object> object) {

        try(final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            final DataOutputStream dos = new DataOutputStream(baos)) {
            schema.serialize(object, dos);
            return baos.toByteArray();
        } catch (final IOException e) {
            throw new IllegalStateException(e);
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

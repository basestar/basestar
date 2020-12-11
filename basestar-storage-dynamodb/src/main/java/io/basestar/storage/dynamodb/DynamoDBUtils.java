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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.basestar.schema.ReferableSchema;
import io.basestar.schema.Reserved;
import io.basestar.secret.Secret;
import io.basestar.util.CompletableFutures;
import io.basestar.util.ISO8601;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.core.BytesWrapper;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.io.*;
import java.time.Instant;
import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@Slf4j
public class DynamoDBUtils {

    public static final AttributeValue EMPTY_STRING_ATTRIBUTE_VALUE = AttributeValue.builder().s("").build();

    public static final AttributeValue EMPTY_BINARY_ATTRIBUTE_VALUE = AttributeValue.builder().b(SdkBytes.fromByteArray(new byte[0])).build();

    public static final int MAX_READ_BATCH_SIZE = 100;

    public static final int MAX_WRITE_BATCH_SIZE = 25;

    public static final int MAX_ITEM_SIZE = 400_000;

    public static Map<String, AttributeValue> toItem(final Map<String, Object> values) {

        return values.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> DynamoDBUtils.toAttributeValue(entry.getValue())));
    }

    public static AttributeValue toAttributeValue(final Object value) {

        if(value == null) {
            return nul(true);
        } else if(value instanceof Boolean) {
            return bool((Boolean)value);
        } else if(value instanceof Number) {
            return n(value.toString());
        } else if(value instanceof String) {
            final String str = (String)value;
            if(str.isEmpty()) {
                return EMPTY_STRING_ATTRIBUTE_VALUE;
            } else {
                return s(str);
            }
        } else if(value instanceof byte[]) {
            final byte[] bytes = (byte[])value;
            if(bytes.length == 0) {
                return EMPTY_BINARY_ATTRIBUTE_VALUE;
            } else {
                return b(bytes);
            }
        } else if(value instanceof Collection) {
            return l(((Collection<?>)value).stream().map(DynamoDBUtils::toAttributeValue)
                            .collect(Collectors.toList()));
        } else if(value instanceof LocalDate) {
            return s(ISO8601.toString((LocalDate)value));
        } else if(value instanceof Instant) {
            return s(ISO8601.toString((Instant)value));
        } else if(value instanceof Map) {
            return m(((Map<?, ?>) value).entrySet().stream()
                    .collect(Collectors.toMap(
                            entry -> entry.getKey().toString(),
                            entry -> toAttributeValue(entry.getValue()))));
        } else if(value instanceof Secret) {
            return b(((Secret) value).encrypted());
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
            return ReferableSchema.deserialize(dis);
        } catch (final IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public static byte[] toOversizeBytes(final ReferableSchema schema, final Map<String, Object> object) {

        try(final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            final DataOutputStream dos = new DataOutputStream(baos)) {
            schema.serialize(object, dos);
            return baos.toByteArray();
        } catch (final IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public static String id(final Map<String, AttributeValue> values) {

        return (String)fromAttributeValue(values.get(ReferableSchema.ID));
    }

    public static Long version(final Map<String, AttributeValue> values) {

        return (Long)fromAttributeValue(values.get(ReferableSchema.VERSION));
    }

    public static String schema(final Map<String, AttributeValue> values) {

        return (String)fromAttributeValue(values.get(ReferableSchema.SCHEMA));
    }

    public static AttributeValue nul(final Boolean v) {

        return AttributeValue.builder().nul(v).build();
    }

    public static AttributeValue bool(final Boolean v) {

        return AttributeValue.builder().bool(v).build();
    }

    public static AttributeValue n(final String v) {

        return AttributeValue.builder().n(v).build();
    }

    public static AttributeValue s(final String v) {

        return AttributeValue.builder().s(v).build();
    }

    public static AttributeValue b(final SdkBytes v) {

        return AttributeValue.builder().b(v).build();
    }

    public static AttributeValue b(final byte[] v) {

        return AttributeValue.builder().b(SdkBytes.fromByteArray(v)).build();
    }

    public static AttributeValue l(final List<AttributeValue> v) {

        return AttributeValue.builder().l(v).build();
    }

    public static AttributeValue m(final Map<String, AttributeValue> v) {

        return AttributeValue.builder().m(v).build();
    }

    private static <T> Collection<T> concat(final Collection<T> a, final Collection<T> b) {

        return ImmutableList.<T>builder().addAll(a).addAll(b).build();
    }

    private static <K, V> Map<K, V> concat(final Map<K, V> a, final Map<K, V> b) {

        return ImmutableMap.<K, V>builder().putAll(a).putAll(b).build();
    }

    private static <T> List<List<T>> slice(final Collection<T> values, final int size) {

        final List<List<T>> slices = new ArrayList<>();
        List<T> slice = null;
        for(final T value : values) {
            if(slice == null || slice.size() == size) {
                slice = new ArrayList<>();
                slices.add(slice);
            }
            slice.add(value);
        }
        return slices;
    }

    public static CompletableFuture<Map<String, Collection<Map<String, AttributeValue>>>> batchRead(final DynamoDbAsyncClient client, final Map<String, ? extends Collection<Map<String, AttributeValue>>> keys) {

        // Not optimum, but will work for now
        final List<CompletableFuture<Map<String, Collection<Map<String, AttributeValue>>>>> futures = keys.entrySet().stream()
                .map(entry -> batchRead(client, entry.getKey(), entry.getValue()).thenApply(v -> Collections.singletonMap(entry.getKey(), v)))
                .collect(Collectors.toList());
        return CompletableFutures.allOf(Collections.emptyMap(), DynamoDBUtils::concat, futures);
    }

    public static CompletableFuture<Collection<Map<String, AttributeValue>>> batchRead(final DynamoDbAsyncClient client, final String tableName, final Collection<Map<String, AttributeValue>> keys) {

        final List<List<Map<String, AttributeValue>>> slices = slice(keys, MAX_READ_BATCH_SIZE);
        final List<CompletableFuture<Collection<Map<String, AttributeValue>>>> futures = slices.stream()
                .map(slice -> batchReadImpl(client, tableName, KeysAndAttributes.builder().keys(slice).build()))
                .collect(Collectors.toList());
        return CompletableFutures.allOf(Collections.emptyList(), DynamoDBUtils::concat, futures);
    }

    private static CompletableFuture<Collection<Map<String, AttributeValue>>> batchReadImpl(final DynamoDbAsyncClient client, final String tableName, final KeysAndAttributes keys) {

        final BatchGetItemRequest req = BatchGetItemRequest.builder()
                .requestItems(Collections.singletonMap(tableName, keys))
                .build();

        return client.batchGetItem(req).thenCompose(results -> {

            final List<Map<String, AttributeValue>> items = results.responses().getOrDefault(tableName, Collections.emptyList());

            final KeysAndAttributes unprocessed = results.hasUnprocessedKeys() ? results.unprocessedKeys().get(tableName) : null;
            if(unprocessed != null) {
                return batchReadImpl(client, tableName, unprocessed)
                        .thenApply(rest -> concat(items, rest));
            } else {
                return CompletableFuture.completedFuture(items);
            }
        });
    }

    public static CompletableFuture<Void> batchWrite(final DynamoDbAsyncClient client, final String tableName, final Collection<WriteRequest> writes) {

        final List<List<WriteRequest>> slices = slice(writes, MAX_WRITE_BATCH_SIZE);
        final List<CompletableFuture<Void>> futures = slices.stream()
                .map(slice -> batchWriteImpl(client, tableName, slice))
                .collect(Collectors.toList());
        return CompletableFutures.allOf(futures).thenApply(v -> null);
    }

    private static CompletableFuture<Void> batchWriteImpl(final DynamoDbAsyncClient client, final String tableName, final Collection<WriteRequest> writes) {

        final BatchWriteItemRequest req = BatchWriteItemRequest.builder()
                .requestItems(Collections.singletonMap(tableName, writes))
                .build();

        return client.batchWriteItem(req).thenCompose(results -> {

            final Collection<WriteRequest> unprocessed = results.hasUnprocessedItems() ? results.unprocessedItems().get(tableName) : null;
            if(unprocessed != null) {
                return batchWriteImpl(client, tableName, unprocessed);
            } else {
                return CompletableFuture.completedFuture(null);
            }
        });
    }

    public static String concat(final String prefix, final String id) {

        if(prefix == null) {
            return id;
        } else {
            return prefix + Reserved.DELIMITER + id;
        }
    }
}

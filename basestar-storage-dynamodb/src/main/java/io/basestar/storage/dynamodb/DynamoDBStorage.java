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

import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.basestar.schema.*;
import io.basestar.storage.*;
import io.basestar.storage.exception.CorruptedIndexException;
import io.basestar.storage.exception.ObjectExistsException;
import io.basestar.storage.exception.UniqueIndexViolationException;
import io.basestar.storage.exception.VersionMismatchException;
import io.basestar.storage.query.Range;
import io.basestar.util.PagedList;
import io.basestar.util.PagingToken;
import io.basestar.util.Path;
import io.basestar.util.Sort;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import javax.annotation.Nonnull;
import java.io.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class DynamoDBStorage extends PartitionedStorage {

    private static final int WRITE_BATCH = 25;

    private static final String OVERSIZE_KEY = Reserved.PREFIX + "oversize";

    private final DynamoDbAsyncClient client;

    private final DynamoDBRouting routing;

    private final Stash oversizeStash;

    private final EventStrategy eventStrategy;

    private DynamoDBStorage(final Builder builder) {

        this.client = builder.client;
        this.routing = builder.routing;
        this.oversizeStash = builder.oversizeStash;
        this.eventStrategy = MoreObjects.firstNonNull(builder.eventStrategy, EventStrategy.EMIT);
    }

    public static Builder builder() {

        return new Builder();
    }

    @Setter
    @Accessors(chain = true)
    public static class Builder {

        private DynamoDbAsyncClient client;

        private DynamoDBRouting routing;

        private Stash oversizeStash;

        private EventStrategy eventStrategy;

        public DynamoDBStorage build() {

            return new DynamoDBStorage(this);
        }
    }

    @Nonnull
    private Stash requireOversizeStash() {

        if(oversizeStash != null) {
            return oversizeStash;
        } else {
            throw new IllegalStateException("Oversize object without oversize stash");
        }
    }

    @Override
    public CompletableFuture<Map<String, Object>> readObject(final ObjectSchema schema, final String id) {

        final GetItemRequest request = GetItemRequest.builder()
                .tableName(routing.objectTableName(schema))
                .key(objectKey(routing, schema, id))
                .build();

        return client.getItem(request)
                .thenCompose(result -> {

                    if(result.item().isEmpty()) {
                        return CompletableFuture.completedFuture(null);
                    } else {
                        return fromItem(result.item());
                    }
                });
    }

    @Override
    public CompletableFuture<Map<String, Object>> readObjectVersion(final ObjectSchema schema, final String id, final long version) {

        final GetItemRequest request = GetItemRequest.builder()
                .tableName(routing.historyTableName(schema))
                .key(historyKey(routing, schema, id, version))
                .build();

        return client.getItem(request)
                .thenCompose(result -> {

                    if(result.item() == null) {
                        return CompletableFuture.completedFuture(null);
                    } else {
                        return fromItem(result.item());
                    }
                });
    }

    private String oversizeStashKey(final ObjectSchema schema, final String id, final long version) {

        return schema.getName() + "/" + id + "/" + version;
    }

    private CompletableFuture<Map<String, Object>> fromItem(final Map<String, AttributeValue> item) {

        final Map<String, Object> object = DynamoDBUtils.fromItem(item);
        final String oversizeKey = checkOversize(object);
        if(oversizeKey != null) {
            return readOversize(oversizeKey);
        } else {
            return CompletableFuture.completedFuture(object);
        }
    }

    private CompletableFuture<Map<String, Object>> readOversize(final String oversizeKey) {

        return requireOversizeStash().read(oversizeKey)
                .thenApply(bytes -> bytes == null ? null : DynamoDBUtils.fromOversizeBytes(bytes));
    }

    private CompletableFuture<BatchResponse> fromItems(final Map<Map<String, AttributeValue>, ObjectSchema> keyToSchema, final Map<String, List<Map<String, AttributeValue>>> items) {

        // could implement read-batching here, but probably not worth it since
        // probable oversize storage engine (S3) doesn't support it meaningfully

        final Map<BatchResponse.Key, CompletableFuture<Map<String, Object>>> oversize = new HashMap<>();
        final Map<BatchResponse.Key, Map<String, Object>> ok = new HashMap<>();
        for(final Map.Entry<String, List<Map<String, AttributeValue>>> entry : items.entrySet()) {
            for (final Map<String, AttributeValue> item : entry.getValue()) {
                final Map<String, Object> object = DynamoDBUtils.fromItem(item);
                final ObjectSchema schema = matchKeyToSchema(keyToSchema, item);
                final BatchResponse.Key key = BatchResponse.Key.from(schema.getName(), object);
                final String oversizeKey = checkOversize(object);
                if (oversizeKey != null) {
                    oversize.put(key, readOversize(oversizeKey));
                } else {
                    ok.put(key, object);
                }
            }
        }

        if(oversize.isEmpty()) {
            return CompletableFuture.completedFuture(new BatchResponse.Basic(ok));
        } else {
            return CompletableFuture.allOf(oversize.values().toArray(new CompletableFuture<?>[0]))
                    .thenApply(ignored -> {
                        final SortedMap<BatchResponse.Key, Map<String, Object>> all = new TreeMap<>(ok);
                        oversize.forEach((key, future) -> all.put(key, future.getNow(all.get(key))));
                        return new BatchResponse.Basic(all);
                    });
        }
    }

    private ObjectSchema matchKeyToSchema(final Map<Map<String, AttributeValue>, ObjectSchema> keyToSchema, final Map<String, AttributeValue> item) {

        for(final Map.Entry<Map<String, AttributeValue>, ObjectSchema> entry : keyToSchema.entrySet()) {
            if(keyMatches(entry.getKey(), item)) {
                return entry.getValue();
            }
        }
        throw new IllegalStateException("Schema not found for key");
    }

    private boolean keyMatches(final Map<String, AttributeValue> key, final Map<String, AttributeValue> item) {

        for(final Map.Entry<String, AttributeValue> entry : key.entrySet()) {
            if(!Objects.equals(entry.getValue(), item.get(entry.getKey()))) {
                return false;
            }
        }
        return true;
    }

    public static String checkOversize(final Map<String, Object> item) {

        return (String)item.get(OVERSIZE_KEY);
    }

    @Override
    protected CompletableFuture<PagedList<Map<String, Object>>> queryIndex(final ObjectSchema schema, final Index index, final SatisfyResult satisfy, final Map<Path, Range<Object>> query, final List<Sort> sort, final int count, final PagingToken paging) {

        final List<Object> mergePartitions = new ArrayList<>();
        mergePartitions.add(routing.indexPartitionPrefix(schema, index));
        mergePartitions.addAll(satisfy.getPartition());

        final SdkBytes partitionValue = SdkBytes.fromByteArray(binary(mergePartitions));

        final Map<String, String> names = new HashMap<>();
        final Map<String, AttributeValue> values = new HashMap<>();

        final List<String> keyTerms = new ArrayList<>();

        keyTerms.add("#__partition = :__partition");
        names.put("#__partition", routing.indexPartitionName(schema, index));
        values.put(":__partition", AttributeValue.builder().b(partitionValue).build());

        if(!satisfy.getSort().isEmpty()) {

            final SdkBytes sortValueLo = SdkBytes.fromByteArray(binary(satisfy.getSort()));
            final SdkBytes sortValueHi = SdkBytes.fromByteArray(binary(satisfy.getSort(), new byte[]{0}));

            keyTerms.add("#__sort BETWEEN :__sortLo AND :__sortHi");
            names.put("#__sort", routing.indexSortName(schema, index));
            values.put(":__sortLo", AttributeValue.builder().b(sortValueLo).build());
            values.put(":__sortHi", AttributeValue.builder().b(sortValueHi).build());
        }

        final String keyExpression = Joiner.on(" AND ").join(keyTerms);

        final DynamoDBExpressionBuilder builder = new DynamoDBExpressionBuilder(satisfy.getMatched());
        query.forEach(builder::and);

        final String filterExpression = builder.getExpression();
        names.putAll(builder.getNames());
        values.putAll(builder.getValues());

        log.debug("Query key=\"{}\", filter=\"{}\", names={}, values={}", keyExpression, filterExpression, names, values);

        QueryRequest.Builder queryBuilder = QueryRequest.builder()
                .tableName(routing.indexTableName(schema, index))
                .keyConditionExpression(keyExpression)
                .filterExpression(filterExpression)
                .expressionAttributeNames(names)
                .expressionAttributeValues(values)
                .scanIndexForward(!satisfy.isReversed())
                .limit(count);

        if(paging != null) {

            queryBuilder = queryBuilder.exclusiveStartKey(decodeIndexPaging(schema, index, partitionValue, paging));
        }

        final QueryRequest request = queryBuilder.build();

        return client.query(request)
                .thenApply(result -> {

                    final List<Map<String, AttributeValue>> items = result.items();

                    // Do not need to apply oversize handler (index records can never be oversize)

                    final List<Map<String, Object>> results = items.stream()
                            .map(DynamoDBUtils::fromItem)
                            .collect(Collectors.toList());

                    final PagingToken nextPaging;
                    if(result.lastEvaluatedKey().isEmpty()) {
                        nextPaging = null;
                    } else {
                        nextPaging = encodeIndexPaging(schema, index, result.lastEvaluatedKey());
                    }

                    return new PagedList<>(results, nextPaging);
                });
    }

    @Override
    public ReadTransaction read(final Consistency consistency) {

        return new ReadTransaction() {

            private final Map<String, List<Map<String, AttributeValue>>> items = new HashMap<>();

            private final Map<Map<String, AttributeValue>, ObjectSchema> keyToSchema = new HashMap<>();

            @Override
            public ReadTransaction readObject(final ObjectSchema schema, final String id) {

                final String tableName = routing.objectTableName(schema);
                final Map<String, AttributeValue> key = objectKey(routing, schema, id);
                keyToSchema.put(key, schema);
                items.computeIfAbsent(tableName, ignored -> new ArrayList<>()).add(key);
                return this;
            }

            @Override
            public ReadTransaction readObjectVersion(final ObjectSchema schema, final String id, final long version) {

                final String tableName = routing.historyTableName(schema);
                final Map<String, AttributeValue> key = historyKey(routing, schema, id, version);
                keyToSchema.put(key, schema);
                items.computeIfAbsent(tableName, ignored -> new ArrayList<>()).add(key);
                return this;
            }

            @Override
            public CompletableFuture<BatchResponse> read() {

                return read(items.entrySet().stream().collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> KeysAndAttributes.builder().keys(entry.getValue()).build()
                )));
            }

            private CompletableFuture<BatchResponse> read(final Map<String, KeysAndAttributes> items) {

                if(items.isEmpty()) {
                    return CompletableFuture.completedFuture(BatchResponse.empty());
                }

                final BatchGetItemRequest request = BatchGetItemRequest.builder()
                        .requestItems(items)
                        .build();

                return client.batchGetItem(request)
                        .thenCompose(result -> read(result.unprocessedKeys())
                                .thenCompose(rest -> fromItems(keyToSchema, result.responses())
                                        .thenApply(from -> BatchResponse.merge(Stream.of(rest, from)))));
            }
        };
    }

    private static Map<String, AttributeValue> oversizeItem(final DynamoDBRouting routing, final ObjectSchema schema, final String id, final Map<String, Object> data, final String key) {

        final Map<String, AttributeValue> item = new HashMap<>();
        item.putAll(DynamoDBUtils.toItem(ObjectSchema.readMeta(data)));
        item.putAll(objectKey(routing, schema, id));
        item.put(OVERSIZE_KEY, AttributeValue.builder().s(key).build());
        return item;
    }

    private static Map<String, AttributeValue> objectItem(final DynamoDBRouting routing, final ObjectSchema schema, final String id, final Map<String, Object> data) {

        final Map<String, AttributeValue> item = new HashMap<>();
        item.putAll(DynamoDBUtils.toItem(data));
        item.putAll(objectKey(routing, schema, id));
        return item;
    }

    private static Map<String, AttributeValue> objectKey(final DynamoDBRouting routing, final ObjectSchema schema, final String id) {

        final String prefix = routing.objectPartitionPrefix(schema);
        final String partition;
        if(prefix == null) {
            partition = id;
        } else {
            partition = prefix + Reserved.DELIMITER + id;
        }
        return ImmutableMap.of(
                routing.objectPartitionName(schema), AttributeValue.builder().s(partition).build()
        );
    }

    private static Map<String, AttributeValue> historyKey(final DynamoDBRouting routing, final ObjectSchema schema, final String id, final long version) {

        final String prefix = routing.historyPartitionPrefix(schema);
        final String partition;
        if(prefix == null) {
            partition = id;
        } else {
            partition = prefix + Reserved.DELIMITER + id;
        }
        return ImmutableMap.of(
                routing.historyPartitionName(schema), AttributeValue.builder().s(partition).build(),
                routing.historySortName(schema), AttributeValue.builder()
                        .n(Long.toString(version)).build()
        );
    }

    private static Map<String, AttributeValue> indexKey(final DynamoDBRouting routing, final ObjectSchema schema, final Index index, final String id, final Index.Key key) {

        return ImmutableMap.of(
                routing.indexPartitionName(schema, index), AttributeValue.builder()
                        .b(SdkBytes.fromByteArray(partition(routing, schema, index, id, key.getPartition()))).build(),
                routing.indexSortName(schema, index), AttributeValue.builder()
                        .b(SdkBytes.fromByteArray(sort(schema, index, id, key.getSort()))).build()
        );
    }

    @SuppressWarnings("unused")
    public static byte[] partition(final DynamoDBRouting routing, final ObjectSchema schema, final Index index, final String id, final List<Object> partition) {

        final String prefix = routing.indexPartitionPrefix(schema, index);
        final List<Object> fullPartition = new ArrayList<>();
        if(prefix != null) {
            fullPartition.add(prefix);
        }
        fullPartition.addAll(partition);
        return binary(fullPartition);
    }

    @SuppressWarnings("unused")
    public static byte[] sort(final ObjectSchema schema, final Index index, final String id, final List<Object> sort) {

        final List<Object> fullSort = new ArrayList<>(sort);
        if(index.isUnique() && sort.isEmpty()) {
            // Must add something to the sort key to save
            fullSort.add(null);
        } else {
            fullSort.add(id);
        }
        return binary(fullSort);
    }

    private static Map<String, AttributeValue> indexItem(final DynamoDBRouting routing, final ObjectSchema schema, final Index index, final String id, final Index.Key key, final Map<String, Object> data) {

        final ImmutableMap.Builder<String, AttributeValue> builder = ImmutableMap.builder();
        builder.putAll(DynamoDBUtils.toItem(data));
        builder.putAll(indexKey(routing, schema, index, id, key));
        return builder.build();
    }

    @Override
    public WriteTransaction write(final Consistency consistency) {

        return new WriteTransaction() {

            private final List<TransactWriteItem> items = new ArrayList<>();

            private final List<Supplier<RuntimeException>> exceptions = new ArrayList<>();

            private final Map<String, byte[]> oversize = new HashMap<>();

            private final SortedMap<BatchResponse.Key, Map<String, Object>> changes = new TreeMap<>();

            private Map<String, AttributeValue> oversize(final ObjectSchema schema, final String id, final Map<String, Object> after) {

                final Long afterVersion = Instance.getVersion(after);
                assert afterVersion != null;
                final String key = oversizeStashKey(schema, id, afterVersion);
                oversize.put(key, DynamoDBUtils.toOversizeBytes(schema, after));
                return oversizeItem(routing, schema, id, after, key);
            }

            @Override
            public WriteTransaction createObject(final ObjectSchema schema, final String id, final Map<String, Object> after) {

                final Map<String, AttributeValue> initialItem = objectItem(routing, schema, id, after);
                final long size = DynamoDBUtils.itemSize(initialItem);
                log.debug("Create object {}:{} ({} bytes)", schema.getName(), id, size);
                final Map<String, AttributeValue> item;
                if(size > DynamoDBUtils.MAX_ITEM_SIZE) {
                    log.info("Creating oversize object {}:{} ({} bytes)", schema.getName(), id, size);
                    item = oversize(schema, id, after);
                } else {
                    item = initialItem;
                }
                items.add(TransactWriteItem.builder()
                        .put(Put.builder()
                                .tableName(routing.objectTableName(schema))
                                .item(item)
                                .conditionExpression("attribute_not_exists(#id)")
                                .expressionAttributeNames(ImmutableMap.of(
                                        "#id", routing.objectPartitionName(schema)
                                ))
                                .build())
                        .build());
                items.add(TransactWriteItem.builder()
                        .put(Put.builder()
                                .tableName(routing.historyTableName(schema))
                                .item(item)
                                .build())
                        .build());

                exceptions.add(() -> new ObjectExistsException(schema.getName(), id));
                exceptions.add(null);
                changes.put(BatchResponse.Key.from(schema.getName(), after), after);

                return createIndexes(schema, id, after);
            }

            @Override
            public WriteTransaction updateObject(final ObjectSchema schema, final String id, final Map<String, Object> before, final Map<String, Object> after) {

                final Long version = before == null ? null : Instance.getVersion(before);
                // FIXME
                assert version != null;

                final Map<String, AttributeValue> initialItem = objectItem(routing, schema, id, after);
                final long size = DynamoDBUtils.itemSize(initialItem);
                log.debug("Updating object {}:{} ({} bytes)", schema.getName(), id, size);
                final Map<String, AttributeValue> item;
                if(size > DynamoDBUtils.MAX_ITEM_SIZE) {
                    log.info("Updating oversize object {}:{} ({} bytes)", schema.getName(), id, size);
                    item = oversize(schema, id, after);
                } else {
                    item = initialItem;
                }
                items.add(TransactWriteItem.builder()
                        .put(Put.builder()
                                .tableName(routing.objectTableName(schema))
                                .item(item)
                                .conditionExpression("#version = :version")
                                .expressionAttributeNames(ImmutableMap.of(
                                        "#version", Reserved.VERSION
                                ))
                                .expressionAttributeValues(ImmutableMap.of(
                                        ":version", AttributeValue.builder().n(Long.toString(version)).build()
                                ))
                                .build())
                        .build());
                items.add(TransactWriteItem.builder()
                        .put(Put.builder()
                                .tableName(routing.historyTableName(schema))
                                .item(item)
                                .build())
                        .build());

                exceptions.add(() -> new VersionMismatchException(schema.getName(), id, version));
                exceptions.add(null);
                changes.put(BatchResponse.Key.from(schema.getName(), after), after);

                return updateIndexes(schema, id, before, after);
            }

            @Override
            public WriteTransaction deleteObject(final ObjectSchema schema, final String id, final Map<String, Object> before) {

                final Long version = before == null ? null : Instance.getVersion(before);
                // FIXME
                assert version != null;

                items.add(TransactWriteItem.builder()
                        .delete(Delete.builder()
                                .tableName(routing.objectTableName(schema))
                                .key(objectKey(routing, schema, id))
                                .conditionExpression("#version = :version")
                                .expressionAttributeNames(ImmutableMap.of(
                                        "#version", Reserved.VERSION
                                ))
                                .expressionAttributeValues(ImmutableMap.of(
                                        ":version", AttributeValue.builder().n(Long.toString(version)).build()
                                ))
                                .build())
                        .build());

                exceptions.add(() -> new VersionMismatchException(schema.getName(), id, version));

                return deleteIndexes(schema, id, before);
            }

            @Override
            public WriteTransaction createIndex(final ObjectSchema schema, final Index index, final String id, final long version, final Index.Key key, final Map<String, Object> projection) {

                items.add(TransactWriteItem.builder()
                        .put(Put.builder()
                                .tableName(routing.indexTableName(schema, index))
                                .item(indexItem(routing, schema, index, id, key, projection))
                                .conditionExpression("attribute_not_exists(#id)")
                                .expressionAttributeNames(ImmutableMap.of(
                                        "#id", routing.indexPartitionName(schema, index)
                                ))
                                .build())
                        .build());

                exceptions.add(() -> new UniqueIndexViolationException(schema.getName(), id, index.getName()));

                return this;
            }

            @Override
            public WriteTransaction updateIndex(final ObjectSchema schema, final Index index, final String id, final long version, final Index.Key key, final Map<String, Object> projection) {

                items.add(TransactWriteItem.builder()
                        .put(Put.builder()
                                .tableName(routing.indexTableName(schema, index))
                                .item(indexItem(routing, schema, index, id, key, projection))
                                .conditionExpression("#version = :version")
                                .expressionAttributeNames(ImmutableMap.of(
                                        "#version", Reserved.VERSION
                                ))
                                .expressionAttributeValues(ImmutableMap.of(
                                        ":version", AttributeValue.builder().n(Long.toString(version)).build()
                                ))
                                .build())
                        .build());

                exceptions.add(() -> new CorruptedIndexException(schema.getName(), index.getName()));

                return this;
            }

            @Override
            public WriteTransaction deleteIndex(final ObjectSchema schema, final Index index, final String id, final long version, final Index.Key key) {

                items.add(TransactWriteItem.builder()
                        .delete(Delete.builder()
                                .tableName(routing.indexTableName(schema, index))
                                .key(indexKey(routing, schema, index, id, key))
                                .conditionExpression("#version = :version")
                                .expressionAttributeNames(ImmutableMap.of(
                                        "#version", Reserved.VERSION
                                ))
                                .expressionAttributeValues(ImmutableMap.of(
                                        ":version", AttributeValue.builder().n(Long.toString(version)).build()
                                ))
                                .build())
                        .build());

                exceptions.add(() -> new CorruptedIndexException(schema.getName(), index.getName()));

                return this;
            }

            @Override
            public Storage.WriteTransaction createHistory(final ObjectSchema schema, final String id, final long version, final Map<String, Object> after) {

                throw new UnsupportedOperationException();
            }

            @Override
            public CompletableFuture<BatchResponse> commit() {

                if(!oversize.isEmpty()) {
                    final Stash oversizeStash = requireOversizeStash();
                    final List<CompletableFuture<?>> futures = new ArrayList<>();
                    oversize.forEach((k, v) -> futures.add(oversizeStash.write(k, v)));
                    return CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[0]))
                            .thenCompose(ignored -> write());
                } else {
                    return write();
                }
            }

            private BatchWriteItemRequest toBatchRequest(final List<TransactWriteItem> items) {

                final Map<String, Collection<WriteRequest>> requests = new HashMap<>();
                for(final TransactWriteItem item : items) {
                    if (item.put() != null) {
                        requests.computeIfAbsent(item.put().tableName(), ignored -> new ArrayList<>())
                                .add(WriteRequest.builder().putRequest(PutRequest.builder()
                                        .item(item.put().item())
                                        .build())
                                .build());
                    } else if (item.delete() != null) {
                        requests.computeIfAbsent(item.delete().tableName(), ignored -> new ArrayList<>())
                                .add(WriteRequest.builder().deleteRequest(DeleteRequest.builder()
                                        .key(item.delete().key())
                                        .build())
                                .build());
                    } else {
                        throw new IllegalStateException();
                    }
                }
                return BatchWriteItemRequest.builder().requestItems(requests).build();
            }

            private CompletableFuture<BatchResponse> write() {

                if(consistency == Consistency.NONE) {

                    return CompletableFuture.allOf(Lists.partition(items, WRITE_BATCH).stream()
                            .map(part -> {
                                final BatchWriteItemRequest request = toBatchRequest(part);
                                return client.batchWriteItem(request);
                            })
                            .toArray(CompletableFuture<?>[]::new))
                            .thenApply(v -> new BatchResponse.Basic(changes));

                } else {

                    final TransactWriteItemsRequest request = TransactWriteItemsRequest.builder()
                            .transactItems(items)
                            .build();

                    return client.transactWriteItems(request)
                            .exceptionally(e -> {
                                if (e.getCause() instanceof TransactionCanceledException) {
                                    final TransactionCanceledException cancel = (TransactionCanceledException) e.getCause();
                                    final List<CancellationReason> reasons = cancel.cancellationReasons();
                                    for (int i = 0; i != reasons.size(); ++i) {
                                        final CancellationReason reason = reasons.get(i);
                                        if ("ConditionalCheckFailed".equals(reason.code())) {
                                            final Supplier<RuntimeException> supplier = exceptions.get(i);
                                            if (supplier != null) {
                                                throw supplier.get();
                                            }
                                        }
                                    }
                                    throw new IllegalStateException(e);
                                } else if (e instanceof RuntimeException) {
                                    throw (RuntimeException) e;
                                } else {
                                    throw new CompletionException(e);
                                }
                            })
                            .thenApply(v -> new BatchResponse.Basic(changes));
                }
            }

        };
    }

    @Override
    public EventStrategy eventStrategy(final ObjectSchema schema) {

        return eventStrategy;
    }

    @Override
    public StorageTraits storageTraits(final ObjectSchema schema) {

        return DynamoDBStorageTraits.INSTANCE;
    }

    private PagingToken encodeIndexPaging(final ObjectSchema schema, final Index index, final Map<String, AttributeValue> key) {

        final byte[] indexSort = key.get(routing.indexSortName(schema, index)).b().asByteArray();

        try(final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            final DataOutputStream dos = new DataOutputStream(baos)) {

            dos.writeShort(indexSort.length);
            dos.write(indexSort);

            return new PagingToken(baos.toByteArray());

        } catch (final IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private Map<String, AttributeValue> decodeIndexPaging(final ObjectSchema schema, final Index index, final SdkBytes partition, final PagingToken paging) {

        try(final ByteArrayInputStream bais = new ByteArrayInputStream(paging.getValue());
            final DataInputStream dis = new DataInputStream(bais)) {

            final int len = dis.readUnsignedShort();
            final byte[] indexSort = new byte[len];
            final int read = dis.read(indexSort);
            assert(read == len);

            return ImmutableMap.of(
                    routing.indexPartitionName(schema, index), AttributeValue.builder().b(partition).build(),
                    routing.indexSortName(schema, index), AttributeValue.builder().b(SdkBytes.fromByteArray(indexSort)).build()
            );

        } catch (final IOException e) {
            throw new IllegalStateException(e);
        }
    }
}

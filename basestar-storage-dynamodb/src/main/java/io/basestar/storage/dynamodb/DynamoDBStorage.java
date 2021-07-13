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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.basestar.expression.Expression;
import io.basestar.schema.*;
import io.basestar.storage.*;
import io.basestar.storage.exception.CorruptedIndexException;
import io.basestar.storage.exception.ObjectExistsException;
import io.basestar.storage.exception.UniqueIndexViolationException;
import io.basestar.storage.exception.VersionMismatchException;
import io.basestar.storage.query.Range;
import io.basestar.util.*;
import lombok.RequiredArgsConstructor;
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

@Slf4j
public class DynamoDBStorage implements DefaultIndexStorage {

    private static final int WRITE_BATCH = 25;

    private static final long EMPTY_SCAN_DELAY_MILLIS = 1000L;

    private static final String OVERSIZE_KEY = Reserved.PREFIX + "oversize";

    private final DynamoDbAsyncClient client;

    private final DynamoDBStrategy strategy;

    private final Stash oversizeStash;

    private final EventStrategy eventStrategy;

    private DynamoDBStorage(final Builder builder) {

        this.client = builder.client;
        this.strategy = builder.strategy;
        this.oversizeStash = builder.oversizeStash;
        this.eventStrategy = Nullsafe.orDefault(builder.eventStrategy, EventStrategy.EMIT);
    }

    public static Builder builder() {

        return new Builder();
    }

    @Setter
    @Accessors(chain = true)
    public static class Builder {

        private DynamoDbAsyncClient client;

        private DynamoDBStrategy strategy;

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

    private String oversizeStashKey(final ReferableSchema schema, final String id, final long version) {

        return schema.getQualifiedName() + "/" + id + "/" + version;
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

    private CompletableFuture<BatchResponse> fromItems(final Map<Map<String, AttributeValue>, ReferableSchema> keyToSchema, final Map<String, ? extends Collection<Map<String, AttributeValue>>> items) {

        // could implement read-batching here, but probably not worth it since
        // probable oversize storage engine (S3) doesn't support it meaningfully

        final Map<BatchResponse.RefKey, CompletableFuture<Map<String, Object>>> oversize = new HashMap<>();
        final Map<BatchResponse.RefKey, Map<String, Object>> ok = new HashMap<>();
        for(final Map.Entry<String, ? extends Collection<Map<String, AttributeValue>>> entry : items.entrySet()) {
            for (final Map<String, AttributeValue> item : entry.getValue()) {
                final Map<String, Object> object = DynamoDBUtils.fromItem(item);
                final ReferableSchema schema = matchKeyToSchema(keyToSchema, item);
                final BatchResponse.RefKey key = BatchResponse.RefKey.from(schema.getQualifiedName(), object);
                final String oversizeKey = checkOversize(object);
                if (oversizeKey != null) {
                    oversize.put(key, readOversize(oversizeKey));
                } else {
                    ok.put(key, object);
                }
            }
        }

        if(oversize.isEmpty()) {
            return CompletableFuture.completedFuture(BatchResponse.fromRefs(ok));
        } else {
            return CompletableFuture.allOf(oversize.values().toArray(new CompletableFuture<?>[0]))
                    .thenApply(ignored -> {
                        final SortedMap<BatchResponse.RefKey, Map<String, Object>> all = new TreeMap<>(ok);
                        oversize.forEach((key, future) -> all.put(key, future.getNow(all.get(key))));
                        return BatchResponse.fromRefs(all);
                    });
        }
    }

    private ReferableSchema matchKeyToSchema(final Map<Map<String, AttributeValue>, ReferableSchema> keyToSchema, final Map<String, AttributeValue> item) {

        for(final Map.Entry<Map<String, AttributeValue>, ReferableSchema> entry : keyToSchema.entrySet()) {
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
    public Pager<Map<String, Object>> queryIndex(final ObjectSchema schema, final Index index, final SatisfyResult satisfy, final Map<Name, Range<Object>> query, final List<Sort> sort, final Set<Name> expand) {

        return (stats, paging, count) -> {

            final List<Object> mergePartitions = new ArrayList<>();
            mergePartitions.add(strategy.indexPartitionPrefix(schema, index));
            mergePartitions.addAll(satisfy.getPartition());

            final SdkBytes partitionValue = SdkBytes.fromByteArray(BinaryKey.from(mergePartitions).getBytes());

            final Map<String, String> names = new HashMap<>();
            final Map<String, AttributeValue> values = new HashMap<>();

            final List<String> keyTerms = new ArrayList<>();

            keyTerms.add("#__partition = :__partition");
            names.put("#__partition", strategy.indexPartitionName(schema, index));
            values.put(":__partition", DynamoDBUtils.b(partitionValue));

            if (!satisfy.getSort().isEmpty()) {

                final BinaryKey sortKey = BinaryKey.from(satisfy.getSort());

                final SdkBytes sortValueLo = SdkBytes.fromByteArray(sortKey.lo().getBytes());
                final SdkBytes sortValueHi = SdkBytes.fromByteArray(sortKey.hi().getBytes());

                keyTerms.add("#__sort BETWEEN :__sortLo AND :__sortHi");
                names.put("#__sort", strategy.indexSortName(schema, index));
                values.put(":__sortLo", DynamoDBUtils.b(sortValueLo));
                values.put(":__sortHi", DynamoDBUtils.b(sortValueHi));
            }

            final String keyExpression = Joiner.on(" AND ").join(keyTerms);

            final DynamoDBExpressionBuilder builder = new DynamoDBExpressionBuilder(satisfy.getMatched());
            query.forEach(builder::and);

            final String filterExpression = builder.getExpression();
            names.putAll(builder.getNames());
            values.putAll(builder.getValues());

            log.debug("Query key=\"{}\", filter=\"{}\", names={}, values={}", keyExpression, filterExpression, names, values);

            QueryRequest.Builder queryBuilder = QueryRequest.builder()
                    .tableName(strategy.indexTableName(schema, index))
                    .keyConditionExpression(keyExpression)
                    .filterExpression(filterExpression)
                    .expressionAttributeNames(names)
                    .expressionAttributeValues(values)
                    .scanIndexForward(!satisfy.isReversed())
                    .limit(count);

            if (paging != null) {

                queryBuilder = queryBuilder.exclusiveStartKey(decodeIndexPagingWithPartition(schema, index, partitionValue.asByteArray(), paging));
            }

            final QueryRequest request = queryBuilder.build();

            return client.query(request)
                    .thenApply(result -> {

                        final List<Map<String, AttributeValue>> items = result.items();

                        // Do not need to apply oversize handler (index records can never be oversize)

                        final List<Map<String, Object>> results = items.stream()
                                .map(DynamoDBUtils::fromItem)
                                .collect(Collectors.toList());

                        final Page.Token nextPaging;
                        if (result.lastEvaluatedKey().isEmpty()) {
                            nextPaging = null;
                        } else {
                            nextPaging = encodeIndexPagingWithoutPartition(schema, index, result.lastEvaluatedKey());
                        }

                        return new Page<>(results, nextPaging);
                    });
        };
    }

    @Override
    public ReadTransaction read(final Consistency consistency) {

        return new ReadTransaction() {

            private final BatchCapture capture = new BatchCapture();

            @Override
            public ReadTransaction getObject(final ObjectSchema schema, final String id, final Set<Name> expand) {

                capture.captureLatest(schema, id, expand);
                return this;
            }

            @Override
            public ReadTransaction getObjectVersion(final ObjectSchema schema, final String id, final long version, final Set<Name> expand) {

                capture.captureVersion(schema, id, version, expand);
                return this;
            }

            @Override
            public CompletableFuture<BatchResponse> read() {

                final Map<String, List<Map<String, AttributeValue>>> items = new HashMap<>();
                final Map<Map<String, AttributeValue>, ReferableSchema> keyToSchema = new HashMap<>();

                capture.forEachRef((schema, ref, args) -> {
                    if(ref.hasVersion()) {
                        final String tableName = strategy.historyTableName(schema);
                        final Map<String, AttributeValue> key = historyKey(strategy, schema, ref.getId(), ref.getVersion());
                        keyToSchema.put(key, schema);
                        items.computeIfAbsent(tableName, ignored -> new ArrayList<>()).add(key);
                    } else {
                        final String tableName = strategy.objectTableName(schema);
                        final Map<String, AttributeValue> key = objectKey(strategy, schema, ref.getId());
                        keyToSchema.put(key, schema);
                        items.computeIfAbsent(tableName, ignored -> new ArrayList<>()).add(key);
                    }
                });

                if(items.isEmpty()) {
                    return CompletableFuture.completedFuture(BatchResponse.empty());
                }

                return DynamoDBUtils.batchRead(client, items)
                                .thenCompose(responses -> fromItems(keyToSchema, responses));
            }
        };
    }

    public static Map<String, AttributeValue> oversizeItem(final DynamoDBStrategy strategy, final ReferableSchema schema, final String id, final Map<String, Object> data, final String key) {

        final Map<String, AttributeValue> item = new HashMap<>();
        item.putAll(DynamoDBUtils.toItem(schema.readMeta(data, false)));
        item.putAll(objectKey(strategy, schema, id));
        item.put(OVERSIZE_KEY, DynamoDBUtils.s(key));
        return item;
    }

    public static Map<String, AttributeValue> objectItem(final DynamoDBStrategy strategy, final ReferableSchema schema, final String id, final Map<String, Object> data) {

        final Map<String, AttributeValue> item = new HashMap<>();
        item.putAll(DynamoDBUtils.toItem(data));
        item.putAll(objectKey(strategy, schema, id));
        return item;
    }

    public static Map<String, AttributeValue> objectKey(final DynamoDBStrategy strategy, final ReferableSchema schema, final String id) {

        final String prefix = strategy.objectPartitionPrefix(schema);
        final String partition = DynamoDBUtils.concat(prefix, id);
        return ImmutableMap.of(
                strategy.objectPartitionName(schema), DynamoDBUtils.s(partition)
        );
    }

    public static Map<String, AttributeValue> historyKey(final DynamoDBStrategy strategy, final ReferableSchema schema, final String id, final long version) {

        final String prefix = strategy.historyPartitionPrefix(schema);
        final String partition = DynamoDBUtils.concat(prefix, id);
        return ImmutableMap.of(
                strategy.historyPartitionName(schema), DynamoDBUtils.s(partition),
                strategy.historySortName(schema), DynamoDBUtils.n(Long.toString(version))
        );
    }

    public static Map<String, AttributeValue> indexKey(final DynamoDBStrategy strategy, final ReferableSchema schema, final Index index, final String id, final Index.Key.Binary key) {

        return ImmutableMap.of(
                strategy.indexPartitionName(schema, index), DynamoDBUtils.b(partition(strategy, schema, index, id, key.getPartition()).getBytes()),
                strategy.indexSortName(schema, index), DynamoDBUtils.b(sort(schema, index, id, key.getSort()).getBytes())
        );
    }

    @SuppressWarnings("unused")
    public static BinaryKey partition(final DynamoDBStrategy strategy, final ReferableSchema schema, final Index index, final String id, final BinaryKey partition) {

        final String prefix = strategy.indexPartitionPrefix(schema, index);
        final List<Object> partitionPrefix = new ArrayList<>();
        if(prefix != null) {
            partitionPrefix.add(prefix);
        }
        return BinaryKey.from(partitionPrefix).concat(partition);
    }

    @SuppressWarnings("unused")
    public static BinaryKey sort(final ReferableSchema schema, final Index index, final String id, final BinaryKey sort) {

        final List<Object> sortSuffix = new ArrayList<>();
        if(index.isUnique()) {
            if(sort.length() == 0) {
                // Must add something to the sort key to save
                sortSuffix.add(null);
            }
        } else {
            // Ensure non-unique indexes have a unique id
            sortSuffix.add(id);
        }
        return sort.concat(BinaryKey.from(sortSuffix));
    }

    public static Map<String, AttributeValue> indexItem(final DynamoDBStrategy strategy, final ReferableSchema schema, final Index index, final String id, final Index.Key.Binary key, final Map<String, Object> data) {

        final ImmutableMap.Builder<String, AttributeValue> builder = ImmutableMap.builder();
        builder.putAll(DynamoDBUtils.toItem(data));
        builder.putAll(indexKey(strategy, schema, index, id, key));
        return builder.build();
    }

    @Override
    public WriteTransaction write(final Consistency consistency, final Versioning versioning) {

        return new WriteTransaction(consistency, versioning);
    }

    @RequiredArgsConstructor
    protected class WriteTransaction implements DefaultIndexStorage.WriteTransaction {

        private final List<TransactWriteItem> items = new ArrayList<>();

        private final List<Supplier<RuntimeException>> exceptions = new ArrayList<>();

        private final Map<String, byte[]> oversize = new HashMap<>();

        private final SortedMap<BatchResponse.RefKey, Map<String, Object>> changes = new TreeMap<>();

        private final Consistency consistency;

        private final Versioning versioning;

        private Map<String, AttributeValue> oversize(final ReferableSchema schema, final String id, final Map<String, Object> after) {

            final Long afterVersion = Instance.getVersion(after);
            assert afterVersion != null;
            final String key = oversizeStashKey(schema, id, afterVersion);
            oversize.put(key, DynamoDBUtils.toOversizeBytes(schema, after));
            return oversizeItem(strategy, schema, id, after, key);
        }

        private Put.Builder conditionalCreate(final String idAttribute, final Versioning versioning) {

            if(versioning.isChecked()) {
                return Put.builder().conditionExpression("attribute_not_exists(#id)")
                        .expressionAttributeNames(ImmutableMap.of(
                                "#id", idAttribute
                        ));
            } else {
                return Put.builder();
            }
        }

        private Put.Builder conditionalUpdate(final Long version, final Versioning versioning) {

            if(versioning.isChecked()) {
                return Put.builder().conditionExpression("#version = :version")
                        .expressionAttributeNames(ImmutableMap.of(
                                "#version", ObjectSchema.VERSION
                        ))
                        .expressionAttributeValues(ImmutableMap.of(
                                ":version", DynamoDBUtils.n(Long.toString(version))
                        ));
            } else {
                return Put.builder();
            }
        }

        private Delete.Builder conditionalDelete(final Long version, final Versioning versioning) {

            if(versioning.isChecked()) {
                return Delete.builder().conditionExpression("#version = :version")
                        .expressionAttributeNames(ImmutableMap.of(
                                "#version", ObjectSchema.VERSION
                        ))
                        .expressionAttributeValues(ImmutableMap.of(
                                ":version", DynamoDBUtils.n(Long.toString(version))
                        ));
            } else {
                return Delete.builder();
            }
        }

        @Override
        public StorageTraits storageTraits(final ReferableSchema schema) {

            return DynamoDBStorage.this.storageTraits(schema);
        }

        @Override
        public void createObjectLayer(final ReferableSchema schema, final String id, final Map<String, Object> after) {

            final Map<String, AttributeValue> initialItem = objectItem(strategy, schema, id, after);
            final long size = DynamoDBUtils.itemSize(initialItem);
            log.debug("Create object {}:{} ({} bytes)", schema.getQualifiedName(), id, size);
            final Map<String, AttributeValue> item;
            if(size > DynamoDBUtils.MAX_ITEM_SIZE) {
                log.info("Creating oversize object {}:{} ({} bytes)", schema.getQualifiedName(), id, size);
                item = oversize(schema, id, after);
            } else {
                item = initialItem;
            }
            items.add(TransactWriteItem.builder()
                    .put(conditionalCreate(strategy.objectPartitionName(schema), versioning)
                            .tableName(strategy.objectTableName(schema))
                            .item(item).build())
                    .build());
//            items.add(TransactWriteItem.builder()
//                    .put(Put.builder()
//                            .tableName(strategy.historyTableName(schema))
//                            .item(item)
//                            .build())
//                    .build());

            exceptions.add(() -> new ObjectExistsException(schema.getQualifiedName(), id));
//            exceptions.add(null);
            changes.put(BatchResponse.RefKey.from(schema.getQualifiedName(), after), after);
        }

        @Override
        public void updateObjectLayer(final ReferableSchema schema, final String id, final Map<String, Object> before, final Map<String, Object> after) {

            final Long version = before == null ? null : Instance.getVersion(before);
            // FIXME
            assert version != null;

            final Map<String, AttributeValue> initialItem = objectItem(strategy, schema, id, after);
            final long size = DynamoDBUtils.itemSize(initialItem);
            log.debug("Updating object {}:{} ({} bytes)", schema.getQualifiedName(), id, size);
            final Map<String, AttributeValue> item;
            if(size > DynamoDBUtils.MAX_ITEM_SIZE) {
                log.info("Updating oversize object {}:{} ({} bytes)", schema.getQualifiedName(), id, size);
                item = oversize(schema, id, after);
            } else {
                item = initialItem;
            }
            items.add(TransactWriteItem.builder()
                    .put(conditionalUpdate(version, versioning)
                            .tableName(strategy.objectTableName(schema))
                            .item(item).build())
                    .build());
//            items.add(TransactWriteItem.builder()
//                    .put(Put.builder()
//                            .tableName(strategy.historyTableName(schema))
//                            .item(item)
//                            .build())
//                    .build());

            exceptions.add(() -> new VersionMismatchException(schema.getQualifiedName(), id, version));
//            exceptions.add(null);
            changes.put(BatchResponse.RefKey.from(schema.getQualifiedName(), after), after);
        }

        @Override
        public void deleteObjectLayer(final ReferableSchema schema, final String id, final Map<String, Object> before) {

            final Long version = before == null ? null : Instance.getVersion(before);
            // FIXME
            assert version != null;

            items.add(TransactWriteItem.builder()
                    .delete(conditionalDelete(version, versioning)
                            .tableName(strategy.objectTableName(schema))
                            .key(objectKey(strategy, schema, id)).build())
                    .build());

            exceptions.add(() -> new VersionMismatchException(schema.getQualifiedName(), id, version));
        }

        @Override
        public void writeHistoryLayer(final ReferableSchema schema, final String id, final Map<String, Object> after) {

            final Map<String, AttributeValue> initialItem = objectItem(strategy, schema, id, after);
            final long size = DynamoDBUtils.itemSize(initialItem);
            log.debug("Create object {}:{} ({} bytes)", schema.getQualifiedName(), id, size);
            final Map<String, AttributeValue> item;
            if(size > DynamoDBUtils.MAX_ITEM_SIZE) {
                log.info("Creating oversize object {}:{} ({} bytes)", schema.getQualifiedName(), id, size);
                item = oversize(schema, id, after);
            } else {
                item = initialItem;
            }

            items.add(TransactWriteItem.builder()
                    .put(Put.builder()
                            .tableName(strategy.historyTableName(schema))
                            .item(item)
                            .build())
                    .build());
            exceptions.add(null);
        }

        @Override
        public void createIndex(final ReferableSchema schema, final Index index, final String id, final long version, final Index.Key key, final Map<String, Object> projection) {

            items.add(TransactWriteItem.builder()
                    .put(conditionalCreate(strategy.indexPartitionName(schema, index), versioning)
                            .tableName(strategy.indexTableName(schema, index))
                            .item(indexItem(strategy, schema, index, id, key.binary(), projection))
                            .build())
                    .build());

            exceptions.add(() -> new UniqueIndexViolationException(schema.getQualifiedName(), id, index.getName()));
        }

        @Override
        public void updateIndex(final ReferableSchema schema, final Index index, final String id, final long version, final Index.Key key, final Map<String, Object> projection) {

            items.add(TransactWriteItem.builder()
                    .put(conditionalUpdate(version, Versioning.UNCHECKED)
                            .tableName(strategy.indexTableName(schema, index))
                            .item(indexItem(strategy, schema, index, id, key.binary(), projection))
                            .build())
                    .build());

            exceptions.add(() -> new CorruptedIndexException(index.getQualifiedName()));
        }

        @Override
        public void deleteIndex(final ReferableSchema schema, final Index index, final String id, final long version, final Index.Key key) {

            items.add(TransactWriteItem.builder()
                    .delete(conditionalDelete(version, Versioning.UNCHECKED)
                            .tableName(strategy.indexTableName(schema, index))
                            .key(indexKey(strategy, schema, index, id, key.binary()))
                            .build())
                    .build());

            exceptions.add(() -> new CorruptedIndexException(index.getQualifiedName()));
        }

        @Override
        @Deprecated
        public WriteTransaction write(final LinkableSchema schema, final Map<String, Object> after) {

            if(schema instanceof ReferableSchema) {
                items.add(TransactWriteItem.builder()
                        .put(Put.builder().tableName(strategy.objectTableName((ReferableSchema) schema))
                                .item(objectItem(strategy, (ReferableSchema) schema, Instance.getId(after), after)).build())
                        .build());

                exceptions.add(null);
            }

            return this;
        }

        @Override
        public CompletableFuture<BatchResponse> write() {

            if(items.isEmpty()) {
                return CompletableFuture.completedFuture(BatchResponse.empty());
            } else if(!oversize.isEmpty()) {
                final Stash oversizeStash = requireOversizeStash();
                final List<CompletableFuture<?>> futures = new ArrayList<>();
                oversize.forEach((k, v) -> futures.add(oversizeStash.write(k, v)));
                return CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[0]))
                        .thenCompose(ignored -> writeImpl());
            } else {
                return writeImpl();
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

        private CompletableFuture<BatchResponse> writeImpl() {

            if(consistency != Consistency.ATOMIC) {

                return CompletableFuture.allOf(Lists.partition(items, WRITE_BATCH).stream()
                        .map(part -> {
                            final BatchWriteItemRequest request = toBatchRequest(part);
                            return client.batchWriteItem(request);
                        })
                        .toArray(CompletableFuture<?>[]::new))
                        .thenApply(v -> new BatchResponse(changes));

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
                        .thenApply(v -> new BatchResponse(changes));
            }
        }
    }

    @Override
    public EventStrategy eventStrategy(final ReferableSchema schema) {

        return eventStrategy;
    }

    @Override
    public StorageTraits storageTraits(final ReferableSchema schema) {

        return DynamoDBStorageTraits.INSTANCE;
    }

    private class ScanSegment implements Scan.Segment {

        private final ReferableSchema schema;

        private final int segments;

        private final int segment;

        private LinkedList<Map<String, AttributeValue>> items;

        private Map<String, AttributeValue> lastEvaluatedKey;

        public ScanSegment(final ReferableSchema schema, final int segments, final int segment) {

            this.schema = schema;
            this.segments = segments;
            this.segment = segment;
        }

        @Override
        public void close() {

            // not required
        }

        private void prepare() {

            while(items == null || (items.isEmpty() && lastEvaluatedKey != null)) {
                final ScanRequest request = ScanRequest.builder()
                        .tableName(strategy.objectTableName(schema))
                        .filterExpression("#schema = :schema")
                        .expressionAttributeValues(ImmutableMap.of(
                                ":schema", AttributeValue.builder().s(schema.getQualifiedName().toString()).build()
                        ))
                        .expressionAttributeNames(ImmutableMap.of(
                                "#schema", ReferableSchema.SCHEMA
                        ))
                        .totalSegments(segments)
                        .segment(segment)
                        .exclusiveStartKey(lastEvaluatedKey)
                        .build();

                final ScanResponse response = client.scan(request).join();
                items = new LinkedList<>(response.items());
                // FIXME: apply filter expression here
                lastEvaluatedKey = response.lastEvaluatedKey();
                if(lastEvaluatedKey.isEmpty()) {
                    lastEvaluatedKey = null;
                } else if(items.isEmpty()) {
                    // Sleep a bit to avoid hitting DDB thresholds
                    try {
                        Thread.sleep(EMPTY_SCAN_DELAY_MILLIS);
                    } catch (final InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        }

        @Override
        public boolean hasNext() {

            prepare();
            return !items.isEmpty();
        }

        @Override
        public Map<String, Object> next() {

            prepare();
            if(items.isEmpty()) {
                throw new NoSuchElementException();
            }
            final Map<String, Object> result = DynamoDBUtils.fromItem(items.getFirst());
            items.pop();
            return result;
        }
    }

    @Override
    public Scan scan(final ReferableSchema schema, final Expression expression, final int segments) {

        return new Scan() {

            @Override
            public int getSegments() {

                return segments;
            }

            @Override
            public Segment segment(final int segment) {

                return new ScanSegment(schema, segments, segment);
            }
        };
    }

    private static void writeWithLength(final DataOutputStream dos, final byte[] bytes) throws IOException {

        dos.writeInt(bytes.length);
        dos.write(bytes);
    }

    private static byte[] readWithLength(final DataInputStream dis) throws IOException {

        final int len = dis.readInt();
        final byte[] buffer = new byte[len];
        final int read = dis.read(buffer);
        assert(read == len);
        return buffer;
    }

    private Map<String, AttributeValue> decodeIndexPaging(final ObjectSchema schema, final Index index, final byte[] indexPartition, final byte[] indexSort) {

        return ImmutableMap.of(
                strategy.indexPartitionName(schema, index), DynamoDBUtils.b(indexPartition),
                strategy.indexSortName(schema, index), DynamoDBUtils.b(indexSort)
        );
    }

    private Page.Token encodeIndexPaging(final ObjectSchema schema, final Index index, final Map<String, AttributeValue> key) {

        final byte[] indexPartition = key.get(strategy.indexPartitionName(schema, index)).b().asByteArray();
        final byte[] indexSort = key.get(strategy.indexSortName(schema, index)).b().asByteArray();

        try(final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            final DataOutputStream dos = new DataOutputStream(baos)) {
            writeWithLength(dos, indexPartition);
            writeWithLength(dos, indexSort);
            return new Page.Token(baos.toByteArray());
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Map<String, AttributeValue> decodeIndexPaging(final ObjectSchema schema, final Index index, final Page.Token paging) {

        try(final ByteArrayInputStream bais = new ByteArrayInputStream(paging.getValue());
            final DataInputStream dis = new DataInputStream(bais)) {
            final byte[] indexPartition = readWithLength(dis);
            final byte[] indexSort = readWithLength(dis);
            return decodeIndexPaging(schema, index, indexPartition, indexSort);
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Page.Token encodeIndexPagingWithoutPartition(final ObjectSchema schema, final Index index, final Map<String, AttributeValue> key) {

        final byte[] indexSort = key.get(strategy.indexSortName(schema, index)).b().asByteArray();

        try(final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            final DataOutputStream dos = new DataOutputStream(baos)) {
            writeWithLength(dos, indexSort);
            return new Page.Token(baos.toByteArray());
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Map<String, AttributeValue> decodeIndexPagingWithPartition(final ObjectSchema schema, final Index index, final byte[] indexPartition, final Page.Token paging) {

        try(final ByteArrayInputStream bais = new ByteArrayInputStream(paging.getValue());
            final DataInputStream dis = new DataInputStream(bais)) {
            final byte[] indexSort = readWithLength(dis);
            return decodeIndexPaging(schema, index, indexPartition, indexSort);
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}

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
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.basestar.schema.*;
import io.basestar.schema.use.UseBinary;
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
import java.util.stream.Stream;

@Slf4j
public class DynamoDBStorage extends PartitionedStorage implements Storage.WithoutWriteHistory, Storage.WithoutExpand {

    private static final int WRITE_BATCH = 25;

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

    @Override
    public CompletableFuture<Map<String, Object>> readObject(final ObjectSchema schema, final String id, final Set<Name> expand) {

        final GetItemRequest request = GetItemRequest.builder()
                .tableName(strategy.objectTableName(schema))
                .key(objectKey(strategy, schema, id))
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
    public CompletableFuture<Map<String, Object>> readObjectVersion(final ObjectSchema schema, final String id, final long version, final Set<Name> expand) {

        final GetItemRequest request = GetItemRequest.builder()
                .tableName(strategy.historyTableName(schema))
                .key(historyKey(strategy, schema, id, version))
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

    private CompletableFuture<BatchResponse> fromItems(final Map<Map<String, AttributeValue>, ObjectSchema> keyToSchema, final Map<String, ? extends Collection<Map<String, AttributeValue>>> items) {

        // could implement read-batching here, but probably not worth it since
        // probable oversize storage engine (S3) doesn't support it meaningfully

        final Map<BatchResponse.Key, CompletableFuture<Map<String, Object>>> oversize = new HashMap<>();
        final Map<BatchResponse.Key, Map<String, Object>> ok = new HashMap<>();
        for(final Map.Entry<String, ? extends Collection<Map<String, AttributeValue>>> entry : items.entrySet()) {
            for (final Map<String, AttributeValue> item : entry.getValue()) {
                final Map<String, Object> object = DynamoDBUtils.fromItem(item);
                final ObjectSchema schema = matchKeyToSchema(keyToSchema, item);
                final BatchResponse.Key key = BatchResponse.Key.from(schema.getQualifiedName(), object);
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
    protected CompletableFuture<Page<Map<String, Object>>> queryIndex(final ObjectSchema schema, final Index index, final SatisfyResult satisfy,
                                                                      final Map<Name, Range<Object>> query, final List<Sort> sort, final Set<Name> expand,
                                                                      final int count, final Page.Token paging) {

        final List<Object> mergePartitions = new ArrayList<>();
        mergePartitions.add(strategy.indexPartitionPrefix(schema, index));
        mergePartitions.addAll(satisfy.getPartition());

        final SdkBytes partitionValue = SdkBytes.fromByteArray(UseBinary.binaryKey(mergePartitions));

        final Map<String, String> names = new HashMap<>();
        final Map<String, AttributeValue> values = new HashMap<>();

        final List<String> keyTerms = new ArrayList<>();

        keyTerms.add("#__partition = :__partition");
        names.put("#__partition", strategy.indexPartitionName(schema, index));
        values.put(":__partition", DynamoDBUtils.b(partitionValue));

        if(!satisfy.getSort().isEmpty()) {

            final SdkBytes sortValueLo = SdkBytes.fromByteArray(UseBinary.concat(UseBinary.binaryKey(satisfy.getSort()), UseBinary.LO_PREFIX));
            final SdkBytes sortValueHi = SdkBytes.fromByteArray(UseBinary.concat(UseBinary.binaryKey(satisfy.getSort()), UseBinary.HI_PREFIX));

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

        if(paging != null) {

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
                    if(result.lastEvaluatedKey().isEmpty()) {
                        nextPaging = null;
                    } else {
                        nextPaging = encodeIndexPagingWithoutPartition(schema, index, result.lastEvaluatedKey());
                    }

                    return new Page<>(results, nextPaging);
                });
    }

    @Override
    public ReadTransaction read(final Consistency consistency) {

        return new ReadTransaction() {

            private final Map<String, List<Map<String, AttributeValue>>> items = new HashMap<>();

            private final Map<Map<String, AttributeValue>, ObjectSchema> keyToSchema = new HashMap<>();

            @Override
            public ReadTransaction readObject(final ObjectSchema schema, final String id, final Set<Name> expand) {

                final String tableName = strategy.objectTableName(schema);
                final Map<String, AttributeValue> key = objectKey(strategy, schema, id);
                keyToSchema.put(key, schema);
                items.computeIfAbsent(tableName, ignored -> new ArrayList<>()).add(key);
                return this;
            }

            @Override
            public ReadTransaction readObjectVersion(final ObjectSchema schema, final String id, final long version, final Set<Name> expand) {

                final String tableName = strategy.historyTableName(schema);
                final Map<String, AttributeValue> key = historyKey(strategy, schema, id, version);
                keyToSchema.put(key, schema);
                items.computeIfAbsent(tableName, ignored -> new ArrayList<>()).add(key);
                return this;
            }

            @Override
            public CompletableFuture<BatchResponse> read() {

                return read(items);
            }

            private CompletableFuture<BatchResponse> read(final Map<String, List<Map<String, AttributeValue>>> items) {

                if(items.isEmpty()) {
                    return CompletableFuture.completedFuture(BatchResponse.empty());
                }

                return DynamoDBUtils.batchRead(client, items)
                                .thenCompose(responses -> fromItems(keyToSchema, responses));
            }
        };
    }

    public static Map<String, AttributeValue> oversizeItem(final DynamoDBStrategy strategy, final ObjectSchema schema, final String id, final Map<String, Object> data, final String key) {

        final Map<String, AttributeValue> item = new HashMap<>();
        item.putAll(DynamoDBUtils.toItem(schema.readMeta(data, false)));
        item.putAll(objectKey(strategy, schema, id));
        item.put(OVERSIZE_KEY, DynamoDBUtils.s(key));
        return item;
    }

    public static Map<String, AttributeValue> objectItem(final DynamoDBStrategy strategy, final ObjectSchema schema, final String id, final Map<String, Object> data) {

        final Map<String, AttributeValue> item = new HashMap<>();
        item.putAll(DynamoDBUtils.toItem(data));
        item.putAll(objectKey(strategy, schema, id));
        return item;
    }

    public static Map<String, AttributeValue> objectKey(final DynamoDBStrategy strategy, final ObjectSchema schema, final String id) {

        final String prefix = strategy.objectPartitionPrefix(schema);
        final String partition = DynamoDBUtils.concat(prefix, id);
        return ImmutableMap.of(
                strategy.objectPartitionName(schema), DynamoDBUtils.s(partition)
        );
    }

    public static Map<String, AttributeValue> historyKey(final DynamoDBStrategy strategy, final ObjectSchema schema, final String id, final long version) {

        final String prefix = strategy.historyPartitionPrefix(schema);
        final String partition = DynamoDBUtils.concat(prefix, id);
        return ImmutableMap.of(
                strategy.historyPartitionName(schema), DynamoDBUtils.s(partition),
                strategy.historySortName(schema), DynamoDBUtils.n(Long.toString(version))
        );
    }

    public static byte[] indexPartitionPrefix(final DynamoDBStrategy strategy, final ObjectSchema schema, final Index index, final byte[] suffix) {

        final String prefix = strategy.indexPartitionPrefix(schema, index);
        return UseBinary.concat(UseBinary.binaryKey(Collections.singletonList(prefix)), suffix);
    }

    public static Map<String, AttributeValue> indexKey(final DynamoDBStrategy strategy, final ObjectSchema schema, final Index index, final String id, final Index.Key.Binary key) {

        return ImmutableMap.of(
                strategy.indexPartitionName(schema, index), DynamoDBUtils.b(partition(strategy, schema, index, id, key.getPartition())),
                strategy.indexSortName(schema, index), DynamoDBUtils.b(sort(schema, index, id, key.getSort()))
        );
    }

    @SuppressWarnings("unused")
    public static byte[] partition(final DynamoDBStrategy strategy, final ObjectSchema schema, final Index index, final String id, final byte[] partition) {

        final String prefix = strategy.indexPartitionPrefix(schema, index);
        final List<Object> partitionPrefix = new ArrayList<>();
        if(prefix != null) {
            partitionPrefix.add(prefix);
        }
        return UseBinary.concat(UseBinary.binaryKey(partitionPrefix), partition);
    }

    @SuppressWarnings("unused")
    public static byte[] sort(final ObjectSchema schema, final Index index, final String id, final byte[] sort) {

        final List<Object> sortSuffix = new ArrayList<>();
        if(index.isUnique()) {
            if(sort.length == 0) {
                // Must add something to the sort key to save
                sortSuffix.add(null);
            }
        } else {
            // Ensure non-unique indexes have a unique id
            sortSuffix.add(id);
        }
        return UseBinary.concat(sort, UseBinary.binaryKey(sortSuffix));
    }

    public static Map<String, AttributeValue> indexItem(final DynamoDBStrategy strategy, final ObjectSchema schema, final Index index, final String id, final Index.Key key, final Map<String, Object> data) {

        final ImmutableMap.Builder<String, AttributeValue> builder = ImmutableMap.builder();
        builder.putAll(DynamoDBUtils.toItem(data));
        builder.putAll(indexKey(strategy, schema, index, id, key.binary()));
        return builder.build();
    }

    @Override
    public WriteTransaction write(final Consistency consistency, final Versioning versioning) {

        return new WriteTransaction(consistency, versioning);
    }

    @RequiredArgsConstructor
    protected class WriteTransaction extends PartitionedStorage.WriteTransaction {

        private final List<TransactWriteItem> items = new ArrayList<>();

        private final List<Supplier<RuntimeException>> exceptions = new ArrayList<>();

        private final Map<String, byte[]> oversize = new HashMap<>();

        private final SortedMap<BatchResponse.Key, Map<String, Object>> changes = new TreeMap<>();

        private final Consistency consistency;

        private final Versioning versioning;

        private Map<String, AttributeValue> oversize(final ObjectSchema schema, final String id, final Map<String, Object> after) {

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
        public PartitionedStorage.WriteTransaction createObject(final ObjectSchema schema, final String id, final Map<String, Object> after) {

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
            items.add(TransactWriteItem.builder()
                    .put(Put.builder()
                            .tableName(strategy.historyTableName(schema))
                            .item(item)
                            .build())
                    .build());

            exceptions.add(() -> new ObjectExistsException(schema.getQualifiedName(), id));
            exceptions.add(null);
            changes.put(BatchResponse.Key.from(schema.getQualifiedName(), after), after);

            return createIndexes(schema, id, after);
        }

        @Override
        public PartitionedStorage.WriteTransaction updateObject(final ObjectSchema schema, final String id, final Map<String, Object> before, final Map<String, Object> after) {

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
            items.add(TransactWriteItem.builder()
                    .put(Put.builder()
                            .tableName(strategy.historyTableName(schema))
                            .item(item)
                            .build())
                    .build());

            exceptions.add(() -> new VersionMismatchException(schema.getQualifiedName(), id, version));
            exceptions.add(null);
            changes.put(BatchResponse.Key.from(schema.getQualifiedName(), after), after);

            return updateIndexes(schema, id, before, after);
        }

        @Override
        public PartitionedStorage.WriteTransaction deleteObject(final ObjectSchema schema, final String id, final Map<String, Object> before) {

            final Long version = before == null ? null : Instance.getVersion(before);
            // FIXME
            assert version != null;

            items.add(TransactWriteItem.builder()
                    .delete(conditionalDelete(version, versioning)
                            .tableName(strategy.objectTableName(schema))
                            .key(objectKey(strategy, schema, id)).build())
                    .build());

            exceptions.add(() -> new VersionMismatchException(schema.getQualifiedName(), id, version));

            return deleteIndexes(schema, id, before);
        }

        @Override
        public PartitionedStorage.WriteTransaction createIndex(final ObjectSchema schema, final Index index, final String id, final long version, final Index.Key key, final Map<String, Object> projection) {

            items.add(TransactWriteItem.builder()
                    .put(conditionalCreate(strategy.indexPartitionName(schema, index), versioning)
                            .tableName(strategy.indexTableName(schema, index))
                            .item(indexItem(strategy, schema, index, id, key, projection))
                            .build())
                    .build());

            exceptions.add(() -> new UniqueIndexViolationException(schema.getQualifiedName(), id, index.getName()));

            return this;
        }

        @Override
        public PartitionedStorage.WriteTransaction updateIndex(final ObjectSchema schema, final Index index, final String id, final long version, final Index.Key key, final Map<String, Object> projection) {

            items.add(TransactWriteItem.builder()
                    .put(conditionalUpdate(version, Versioning.UNCHECKED)
                            .tableName(strategy.indexTableName(schema, index))
                            .item(indexItem(strategy, schema, index, id, key, projection))
                            .build())
                    .build());

            exceptions.add(() -> new CorruptedIndexException(index.getQualifiedName()));

            return this;
        }

        @Override
        public PartitionedStorage.WriteTransaction deleteIndex(final ObjectSchema schema, final Index index, final String id, final long version, final Index.Key key) {

            items.add(TransactWriteItem.builder()
                    .delete(conditionalDelete(version, Versioning.UNCHECKED)
                            .tableName(strategy.indexTableName(schema, index))
                            .key(indexKey(strategy, schema, index, id, key.binary()))
                            .build())
                    .build());

            exceptions.add(() -> new CorruptedIndexException(index.getQualifiedName()));

            return this;
        }

        @Override
        public CompletableFuture<BatchResponse> write() {

            if(!oversize.isEmpty()) {
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
    }

    @Override
    public EventStrategy eventStrategy(final ObjectSchema schema) {

        return eventStrategy;
    }

    @Override
    public StorageTraits storageTraits(final ObjectSchema schema) {

        return DynamoDBStorageTraits.INSTANCE;
    }

    @Override
    public List<Pager.Source<RepairInfo>> repair(final ObjectSchema schema) {

        return Stream.concat(
                Stream.of(fixObjects(schema, schema.getIndexes().values())),
                schema.getIndexes().values().stream().map(index -> cleanIndex(schema, index))
        ).collect(Collectors.toList());
    }

    @Override
    public List<Pager.Source<RepairInfo>> repairIndex(final ObjectSchema schema, final Index index) {

        return ImmutableList.of(
                fixObjects(schema, Collections.singleton(index)),
                cleanIndex(schema, index)
        );
    }

    private Map<Map<String, AttributeValue>, Map<String, AttributeValue>> indexRecords(final ObjectSchema schema, final Index index, final List<Map<String, AttributeValue>> items) {

        final Map<Map<String, AttributeValue>, Map<String, AttributeValue>> indexRecords = new HashMap<>();
        items.forEach(item -> {
            final Map<String, Object> instance = schema.create(DynamoDBUtils.fromItem(item), schema.getExpand(), true);
            final String id = Instance.getId(instance);
            index.readValues(instance).forEach((key, record) -> {
                final Map<String, AttributeValue> indexKey = indexKey(strategy, schema, index, id, key.binary());
                final Map<String, AttributeValue> indexValues = indexItem(strategy, schema, index, id, key, record);
                indexRecords.put(indexKey, indexValues);
            });
        });
        return indexRecords;
    }

    // Scan objects, create or update missing/incorrect records
    protected Pager.Source<RepairInfo> fixObjects(final ObjectSchema schema, final Collection<Index> indexes) {

        return (count, paging, stats) -> {

            final ScanRequest req = ScanRequest.builder()
                    .tableName(strategy.objectTableName(schema))
                    .limit(count).filterExpression("#schema = :schema")
                    .expressionAttributeNames(Collections.singletonMap("#schema", ObjectSchema.SCHEMA))
                    .expressionAttributeValues(Collections.singletonMap(":schema", DynamoDBUtils.s(schema.getQualifiedName().toString())))
                    .exclusiveStartKey(paging == null ? null : decodeScanPaging(schema, paging))
                    .build();

            return client.scan(req).thenCompose(objects -> {

                final Map<String, Map<Map<String, AttributeValue>, Map<String, AttributeValue>>> indexRecords = new HashMap<>();
                indexes.forEach(index -> indexRecords.put(strategy.indexTableName(schema, index), indexRecords(schema, index, objects.items())));

                final List<CompletableFuture<RepairInfo>> futures = new ArrayList<>();
                indexRecords.forEach((indexTableName, afterRecords) -> {
                    futures.add(DynamoDBUtils.batchRead(client, indexTableName, afterRecords.keySet()).thenCompose(reads -> {

                        // Match reads back to their keys
                        final Map<Map<String, AttributeValue>, Map<String, AttributeValue>> beforeRecords = new HashMap<>();
                        afterRecords.keySet().forEach(key -> reads.forEach(read -> {
                            if(key.entrySet().stream().allMatch(entry -> Objects.equals(entry.getValue(), read.get(entry.getKey())))) {
                                assert(!beforeRecords.containsKey(key));
                                beforeRecords.put(key, read);
                            }
                        }));

                        final List<WriteRequest> writes = new ArrayList<>();
                        final RepairInfo sum = afterRecords.entrySet().stream().map(entry -> {
                            final Map<String, AttributeValue> key = entry.getKey();
                            final Map<String, AttributeValue> afterRecord = entry.getValue();
                            final Map<String, AttributeValue> beforeRecord = beforeRecords.get(key);
                            if(!Objects.equals(afterRecord, beforeRecord)) {
                                writes.add(WriteRequest.builder()
                                        .putRequest(PutRequest.builder()
                                                .item(afterRecord)
                                                .build())
                                        .build());
                                if(beforeRecord == null) {
                                    log.debug("Creating missing index value: {} {}", indexTableName, key);
                                    return RepairInfo.ZERO.withCreatedIndexRecords(1);
                                } else {
                                    log.debug("Overwriting corrupted index value: {} {}", indexTableName, key);
                                    return RepairInfo.ZERO.withUpdatedIndexRecords(1);
                                }
                            } else {
                                return RepairInfo.ZERO;
                            }
                        }).reduce(RepairInfo.ZERO, RepairInfo::sum);

                        return DynamoDBUtils.batchWrite(client, indexTableName, writes)
                                .thenApply(ignored -> sum);
                    }));
                });

                return CompletableFutures.allOf(RepairInfo.ZERO.withScannedObjects(objects.scannedCount()), RepairInfo::sum, futures).thenApply(sum -> {

                    final Page.Token nextPaging;
                    if (objects.hasLastEvaluatedKey()) {
                        nextPaging = encodeScanPaging(schema, objects.lastEvaluatedKey());
                    } else {
                        nextPaging = null;
                    }
                    return new Page<>(Collections.singletonList(sum), nextPaging);
                });
            });
        };
    }

    // Scan index, delete redundant records
    protected Pager.Source<RepairInfo> cleanIndex(final ObjectSchema schema, final Index index) {

        if(strategy.indexType(schema, index) == DynamoDBStrategy.IndexType.EXT) {

            final String objectTableName = strategy.objectTableName(schema);
            final String indexTableName = strategy.indexTableName(schema, index);
            final String indexPartitionName = strategy.indexPartitionName(schema, index);
            final String indexSortName = strategy.indexSortName(schema, index);

            return (count, paging, stats) -> {

                final ScanRequest req = ScanRequest.builder()
                        .tableName(indexTableName)
                        .limit(count).filterExpression("#schema = :schema AND #partition BETWEEN :lo and :hi")
                        .expressionAttributeNames(ImmutableMap.of(
                                "#schema", ObjectSchema.SCHEMA,
                                "#partition", indexPartitionName
                        ))
                        .expressionAttributeValues(ImmutableMap.of(
                                ":schema", DynamoDBUtils.s(schema.getQualifiedName().toString()),
                                ":lo", DynamoDBUtils.b(indexPartitionPrefix(strategy, schema, index, UseBinary.LO_PREFIX)),
                                ":hi", DynamoDBUtils.b(indexPartitionPrefix(strategy, schema, index, UseBinary.HI_PREFIX))
                        ))
                        .exclusiveStartKey(paging == null ? null : decodeIndexPaging(schema, index, paging))
                        .build();

                return client.scan(req).thenCompose(results -> {

                    final Set<Map<String, AttributeValue>> objectKeys = new HashSet<>();

                    results.items().forEach(item -> {
                        final String id = DynamoDBUtils.id(item);
                        objectKeys.add(objectKey(strategy, schema, id));
                    });

                    return DynamoDBUtils.batchRead(client, objectTableName, objectKeys).thenCompose(objects -> {

                        final Map<String, Set<Map<String, AttributeValue>>> expected = new HashMap<>();
                        objects.forEach(item -> {
                            final Map<String, Object> data = DynamoDBUtils.fromItem(item);
                            final Map<String, Object> object = schema.create(data, schema.getExpand(), true);
                            final String id = Instance.getId(object);
                            final Set<Map<String, AttributeValue>> keys = new HashSet<>();
                            index.readValues(object).forEach((key, record) -> keys.add(indexKey(strategy, schema, index, id, key.binary())));
                            expected.put(id, keys);
                        });

                        final Set<Map<String, AttributeValue>> deletes = new HashSet<>();

                        results.items().forEach(item -> {
                            final String id = DynamoDBUtils.id(item);
                            final Set<Map<String, AttributeValue>> keys = expected.get(id);
                            final Map<String, AttributeValue> indexKey = ImmutableMap.of(
                                    indexPartitionName, item.get(indexPartitionName),
                                    indexSortName, item.get(indexSortName)
                            );
                            if(keys == null || !keys.contains(indexKey)) {
                                log.debug("Deleting corrupted index value: {} {}", indexTableName, indexKey);
                                deletes.add(indexKey);
                            }
                        });

                        final List<WriteRequest> writes = deletes.stream().map(key -> WriteRequest.builder()
                                .deleteRequest(DeleteRequest.builder().key(key).build())
                                .build()).collect(Collectors.toList());

                        return DynamoDBUtils.batchWrite(client, indexTableName, writes)
                                .thenApply(ignored -> RepairInfo.ZERO.withDeletedIndexRecords(writes.size()));

                    }).thenApply(deletes -> {

                        final RepairInfo sum = RepairInfo.sum(deletes, RepairInfo.ZERO.withScannedIndexRecords(results.scannedCount()));

                        final Page.Token nextPaging;
                        if (results.hasLastEvaluatedKey()) {
                            nextPaging = encodeIndexPaging(schema, index, results.lastEvaluatedKey());
                        } else {
                            nextPaging = null;
                        }
                        return new Page<>(Collections.singletonList(sum), nextPaging);

                    });
                });
            };

        } else {
            return Pager.Source.empty();
        }
    }

    private Map<String, AttributeValue> decodeScanPaging(final ObjectSchema schema, final Page.Token paging) {

        try(final ByteArrayInputStream bais = new ByteArrayInputStream(paging.getValue());
            final DataInputStream dis = new DataInputStream(bais)) {
            final byte[] partition = readWithLength(dis);
            return ImmutableMap.of(
                    strategy.objectPartitionName(schema), DynamoDBUtils.s(new String(partition, Charsets.UTF_8))
            );
        } catch (final IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private Page.Token encodeScanPaging(final ObjectSchema schema, final Map<String, AttributeValue> key) {

        final byte[] partition = key.get(strategy.objectPartitionName(schema)).s().getBytes(Charsets.UTF_8);

        try(final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            final DataOutputStream dos = new DataOutputStream(baos)) {
            writeWithLength(dos, partition);
            return new Page.Token(baos.toByteArray());
        } catch (final IOException e) {
            throw new IllegalStateException(e);
        }
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
            throw new IllegalStateException(e);
        }
    }

    private Map<String, AttributeValue> decodeIndexPaging(final ObjectSchema schema, final Index index, final Page.Token paging) {

        try(final ByteArrayInputStream bais = new ByteArrayInputStream(paging.getValue());
            final DataInputStream dis = new DataInputStream(bais)) {
            final byte[] indexPartition = readWithLength(dis);
            final byte[] indexSort = readWithLength(dis);
            return decodeIndexPaging(schema, index, indexPartition, indexSort);
        } catch (final IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private Page.Token encodeIndexPagingWithoutPartition(final ObjectSchema schema, final Index index, final Map<String, AttributeValue> key) {

        final byte[] indexSort = key.get(strategy.indexSortName(schema, index)).b().asByteArray();

        try(final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            final DataOutputStream dos = new DataOutputStream(baos)) {
            writeWithLength(dos, indexSort);
            return new Page.Token(baos.toByteArray());
        } catch (final IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private Map<String, AttributeValue> decodeIndexPagingWithPartition(final ObjectSchema schema, final Index index, final byte[] indexPartition, final Page.Token paging) {

        try(final ByteArrayInputStream bais = new ByteArrayInputStream(paging.getValue());
            final DataInputStream dis = new DataInputStream(bais)) {
            final byte[] indexSort = readWithLength(dis);
            return decodeIndexPaging(schema, index, indexPartition, indexSort);
        } catch (final IOException e) {
            throw new IllegalStateException(e);
        }
    }
}

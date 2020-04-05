package io.basestar.storage;

import com.google.common.base.Charsets;
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.schema.Consistency;
import io.basestar.schema.Index;
import io.basestar.schema.Instance;
import io.basestar.schema.ObjectSchema;
import io.basestar.storage.query.DisjunctionVisitor;
import io.basestar.storage.query.Range;
import io.basestar.storage.query.RangeVisitor;
import io.basestar.storage.util.IndexRecordDiff;
import io.basestar.storage.util.Pager;
import io.basestar.util.PagedList;
import io.basestar.util.PagingToken;
import io.basestar.util.Path;
import io.basestar.util.Sort;
import lombok.Data;

import javax.annotation.Nonnull;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public abstract class PartitionedStorage implements Storage {

    protected abstract CompletableFuture<PagedList<Map<String, Object>>> queryIndex(ObjectSchema schema, Index index, SatisfyResult satisfyResult, Map<Path, Range<Object>> query, List<Sort> sort, int count, PagingToken paging);

    @Override
    @SuppressWarnings("unchecked")
    public List<Pager.Source<Map<String, Object>>> query(final ObjectSchema schema, final Expression expression, final List<Sort> sort) {

        final Expression bound = expression.bind(Context.init());
        final Set<Expression> disjunction = bound.visit(new DisjunctionVisitor());

        final List<Pager.Source<Map<String, Object>>> queries = new ArrayList<>();

        List<Sort> indexSort = null;
        for (final Expression conjunction : disjunction) {
            final Map<Path, Range<Object>> query = new HashMap<>();
            for(final Map.Entry<Path, Range<Object>> entry : conjunction.visit(new RangeVisitor()).entrySet()) {
                final Path path = entry.getKey();
//                if(path.isChild(Path.of(Reserved.THIS))) {
                    query.put(path, entry.getValue());
//                }
            }

            SatisfyResult bestSatisfy = SatisfyResult.unsatisfied();
            Index bestIndex = null;
            for (final Index checkIndex : schema.getAllIndexes().values()) {
                final SatisfyResult checkSatisfy = satisfy(checkIndex, query, sort);
                if (checkSatisfy.isSatisfied() && checkSatisfy.compareTo(bestSatisfy) > 0) {
                    bestSatisfy = checkSatisfy;
                    bestIndex = checkIndex;
                }
            }
            if (bestIndex != null) {

                // FIXME
                if(indexSort == null) {
                    indexSort = bestIndex.getSort();
                }

                final Index index = bestIndex;
                final SatisfyResult satisfy = bestSatisfy;
                queries.add((c, p) -> queryIndex(schema, index, satisfy, query, sort, c, p));

            } else {
                throw new IllegalStateException("no index");
            }
        }

        return queries;
    }

    protected SatisfyResult satisfy(final Index index, final Map<Path, Range<Object>> query, final List<Sort> sort) {

        final Map<Path, Object> constants = new HashMap<>();
        query.forEach((k, v) -> {
            if(v instanceof Range.Eq) {
                constants.put(k, ((Range.Eq<?>) v).getEq());
            }
        });

        final List<Path> partition;
        final Map<String, Path> over = index.getOver();
        if(over.isEmpty()) {
            partition = index.getPartition();
        } else {
            partition = index.getPartition().stream()
                    .map(v -> {
                        final Path overPath = over.get(v.first());
                        if(overPath != null) {
                            return overPath.with(v.withoutFirst());
                        } else {
                            return v;
                        }
                    })
                    .collect(Collectors.toList());
        }

        final Set<Path> matched = new HashSet<>();
        final List<Object> partitionValues = new ArrayList<>();
        for(final Path path : partition) {
            if(constants.containsKey(path)) {
                partitionValues.add(constants.get(path));
                matched.add(path);
            } else {
                return SatisfyResult.unsatisfied();
            }
        }
        final List<Object> sortValues = new ArrayList<>();
        for(final Sort s : index.getSort()) {
            final Path path = s.getPath();
            if(constants.containsKey(path)) {
                partitionValues.add(constants.get(path));
                matched.add(path);
            } else {
                break;
            }
        }
        int matchedSort = sortValues.size();
        boolean reversed = false;
        for(final Sort thatSort : sort) {
            if(!constants.containsKey(thatSort.getPath())) {
                if (matchedSort < index.getSort().size()) {
                    final Sort thisSort = index.getSort().get(matchedSort);
                    if (thisSort.getPath().equals(thatSort.getPath())) {
                        if ((reversed ? thisSort.getOrder().reverse() : thisSort).equals(thatSort.getOrder())) {
                            ++matchedSort;
                            continue;
                        } else if (!reversed) {
                            reversed = true;
                            ++matchedSort;
                            continue;
                        }
                    }
                }
            }
            // FIXME
            //return SatisfyResult.unsatisfied();
        }
        return new SatisfyResult(partitionValues, sortValues, reversed, matched);
    }

    public static byte[] binary(final List<?> keys) {

        return binary(keys, null);
    }

    public static byte[] binary(final List<?> keys, final byte[] suffix) {

        final byte T_NULL = 0;
        final byte T_FALSE = 1;
        final byte T_TRUE = 2;
        final byte T_INT = 3;
        final byte T_STRING = 4;
        final byte T_BYTES = 5;

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();

        try {
            for(final Object v : keys) {
                if(v == null) {
                    baos.write(T_NULL);
                } else if(v instanceof Boolean) {
                    baos.write(((Boolean)v) ? T_TRUE : T_FALSE);
                } else if(v instanceof Integer || v instanceof Long) {
                    final ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
                    buffer.putLong(((Number)v).longValue());
                    final byte[] bytes = buffer.array();
                    baos.write(T_INT);
                    baos.write(bytes);
                } else if(v instanceof String) {
                    baos.write(T_STRING);
                    baos.write(((String) v).getBytes(Charsets.UTF_8));
                } else if(v instanceof byte[]) {
                    baos.write(T_BYTES);
                    baos.write(((byte[]) v));
                } else {
                    throw new IllegalStateException("Cannot convert " + v.getClass() + " to binary");
                }
            }

            if(suffix != null) {
                baos.write(suffix);
            }

        } catch (final IOException e) {
            throw new IllegalStateException(e);
        }

        return baos.toByteArray();
    }

    @Data
    protected static class SatisfyResult implements Comparable<SatisfyResult> {

        private final List<Object> partition;

        private final List<Object> sort;

        private final boolean reversed;

        private final Set<Path> matched;

        public static SatisfyResult unsatisfied() {

            return new SatisfyResult(Collections.emptyList(), Collections.emptyList(),
                    false, Collections.emptySet());
        }

        public boolean isSatisfied() {

            return partition != null;
        }

        public boolean isMatched(final Path path) {

            return matched != null && matched.contains(path);
        }

        @Override
        public int compareTo(@Nonnull final SatisfyResult other) {

            if(partition.size() == other.partition.size()) {
                return Integer.compare(sort.size(), other.sort.size());
            } else {
                return Integer.compare(partition.size(), other.partition.size());
            }
        }
    }

    protected abstract class WriteTransaction implements Storage.WriteTransaction {

//        protected abstract void createObjectImpl(ObjectSchema schema, String id, Map<String, Object> after);
//
//        protected abstract void updateObjectImpl(ObjectSchema schema, String id, long version, Map<String, Object> before, Map<String, Object> after);
//
//        protected abstract void deleteObjectImpl(ObjectSchema schema, String id, long version, Map<String, Object> before);
//
//        protected abstract CompletableFuture<BatchResponse> commitImpl();
//
//        @Override
//        public WriteTransaction createObject(final ObjectSchema schema, final String id, final Map<String, Object> after) {
//
//            final StorageTraits traits = storageTraits(schema);
//            createObjectImpl(schema, id, after);
//
//            schema.getAllIndexes().forEach((indexName, index) -> {
//                final Consistency best = traits.getIndexConsistency(index.isMultiValue());
//                if(!index.getConsistency(best).isAsync()) {
//                    final Map<Index.Key, Map<String, Object>> records = index.readValues(after);
//                    records.forEach((key, projection) -> createIndex(schema, index, id, 0L, key, projection));
//                }
//            });
//
//            return this;
//        }
//
//        @Override
//        public WriteTransaction updateObject(final ObjectSchema schema, final String id, final long version, final Map<String, Object> before, final Map<String, Object> after) {
//
//            final StorageTraits traits = storageTraits(schema);
//            updateObjectImpl(schema, id, version, before, after);
//
//            schema.getAllIndexes().forEach((indexName, index) -> {
//                final Consistency best = traits.getIndexConsistency(index.isMultiValue());
//                if(!index.getConsistency(best).isAsync()) {
//                    final IndexRecordDiff diff = IndexRecordDiff.from(index.readValues(before), index.readValues(after));
//                    diff.getCreate().forEach((key, projection) -> createIndex(schema, index, id, version, key, projection));
//                    diff.getUpdate().forEach((key, projection) -> updateIndex(schema, index, id, version, key, projection));
//                    diff.getDelete().forEach((key) -> deleteIndex(schema, index, id, version, key));
//                }
//            });
//
//            return this;
//        }
//
//        @Override
//        public WriteTransaction deleteObject(final ObjectSchema schema, final String id, final long version, final Map<String, Object> before) {
//
//            final StorageTraits traits = storageTraits(schema);
//            deleteObjectImpl(schema, id, version, before);
//
//            schema.getAllIndexes().forEach((indexName, index) -> {
//                final Consistency best = traits.getIndexConsistency(index.isMultiValue());
//                if(!index.getConsistency(best).isAsync()) {
//                    final Map<Index.Key, Map<String, Object>> records = index.readValues(before);
//                    records.keySet().forEach((key) -> deleteIndex(schema, index, id, version, key));
//                }
//            });
//
//            return this;
//        }

        protected WriteTransaction createIndexes(final ObjectSchema schema, final String id, final Map<String, Object> after) {

            final StorageTraits traits = storageTraits(schema);
            schema.getAllIndexes().forEach((indexName, index) -> {
                final Consistency best = traits.getIndexConsistency(index.isMultiValue());
                if(!index.getConsistency(best).isAsync()) {
                    final Map<Index.Key, Map<String, Object>> records = index.readValues(after);
                    records.forEach((key, projection) -> createIndex(schema, index, id, 0L, key, projection));
                }
            });

            return this;
        }

        protected WriteTransaction updateIndexes(final ObjectSchema schema, final String id, final Map<String, Object> before, final Map<String, Object> after) {

            final Map<String, Index> indexes = schema.getAllIndexes();
            if(!indexes.isEmpty()) {
                final StorageTraits traits = storageTraits(schema);
                final Long version = Instance.getVersion(before);
                assert version != null;
                indexes.forEach((indexName, index) -> {
                    final Consistency best = traits.getIndexConsistency(index.isMultiValue());
                    if (!index.getConsistency(best).isAsync()) {
                        final IndexRecordDiff diff = IndexRecordDiff.from(index.readValues(before), index.readValues(after));
                        diff.getCreate().forEach((key, projection) -> createIndex(schema, index, id, version, key, projection));
                        diff.getUpdate().forEach((key, projection) -> updateIndex(schema, index, id, version, key, projection));
                        diff.getDelete().forEach((key) -> deleteIndex(schema, index, id, version, key));
                    }
                });
            }
            return this;
        }

        protected WriteTransaction deleteIndexes(final ObjectSchema schema, final String id, final Map<String, Object> before) {

            final Map<String, Index> indexes = schema.getAllIndexes();
            if(!indexes.isEmpty()) {
                final StorageTraits traits = storageTraits(schema);
                final Long version = Instance.getVersion(before);
                assert version != null;
                indexes.forEach((indexName, index) -> {
                    final Consistency best = traits.getIndexConsistency(index.isMultiValue());
                    if (!index.getConsistency(best).isAsync()) {
                        final Map<Index.Key, Map<String, Object>> records = index.readValues(before);
                        records.keySet().forEach((key) -> deleteIndex(schema, index, id, version, key));
                    }
                });
            }
            return this;
        }

//        @Override
//        public CompletableFuture<BatchResponse> commit() {
//
//            return commitImpl();
//        }
    }

//    @Override
//    public CompletableFuture<Set<Event>> onObjectCreated(final ObjectSchema schema, final String id, final Map<String, Object> after) {
//
//        return CompletableFuture.completedFuture(schema.getIndexes().values().stream().flatMap(index -> {
//            if(index.getConsistency().isAsync()) {
//                final Map<Index.Key, Map<String, Object>> records = index.readValues(after);
//                return records.entrySet().stream()
//                        .map(e -> AsyncIndexCreatedEvent.of(schema.getName(), id, 0L, e.getKey(), e.getValue()));
//            } else {
//                return Stream.empty();
//            }
//        }).collect(Collectors.toSet()));
//    }
//
//    @Override
//    public CompletableFuture<Set<Event>> onObjectUpdated(final ObjectSchema schema, final String id, final long version, final Map<String, Object> before, final Map<String, Object> after) {
//
//        return CompletableFuture.completedFuture(schema.getIndexes().values().stream().flatMap(index -> {
//            if(index.getConsistency().isAsync()) {
//                final IndexRecordDiff diff = IndexRecordDiff.from(index.readValues(before), index.readValues(after));
//                final Stream<Event> create = diff.getCreate().entrySet().stream()
//                        .map(e -> AsyncIndexCreatedEvent.of(schema.getName(), id, version, e.getKey(), e.getValue()));
//                final Stream<Event> update = diff.getUpdate().entrySet().stream()
//                        .map(e -> AsyncIndexUpdatedEvent.of(schema.getName(), id, version, e.getKey(), e.getValue()));
//                final Stream<Event> delete = diff.getDelete().stream()
//                        .map(key-> AsyncIndexDeletedEvent.of(schema.getName(), id, version, key));
//                return Stream.of(create, update, delete)
//                        .flatMap(v -> v);
//            } else {
//                return Stream.empty();
//            }
//        }).collect(Collectors.toSet()));
//    }
//
//    @Override
//    public CompletableFuture<Set<Event>> onObjectDeleted(final ObjectSchema schema, final String id, final long version, final Map<String, Object> before) {
//
//        return CompletableFuture.completedFuture(schema.getIndexes().values().stream().flatMap(index -> {
//            if(index.getConsistency().isAsync()) {
//                final Map<Index.Key, Map<String, Object>> records = index.readValues(before);
//                return records.keySet().stream()
//                        .map(key -> AsyncIndexDeletedEvent.of(schema.getName(), id, version, key));
//            } else {
//                return Stream.empty();
//            }
//        }).collect(Collectors.toSet()));
//    }
}

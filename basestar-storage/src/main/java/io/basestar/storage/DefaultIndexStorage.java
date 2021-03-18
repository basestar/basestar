package io.basestar.storage;

import io.basestar.event.Event;
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.schema.*;
import io.basestar.storage.exception.UnsupportedQueryException;
import io.basestar.storage.query.DisjunctionVisitor;
import io.basestar.storage.query.Range;
import io.basestar.storage.query.RangeVisitor;
import io.basestar.util.Name;
import io.basestar.util.Pager;
import io.basestar.util.Sort;
import lombok.Data;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public interface DefaultIndexStorage extends IndexStorage, DefaultLayerStorage {

    @Override
    ReadTransaction read(Consistency consistency);

    @Override
    WriteTransaction write(Consistency consistency, Versioning versioning);


    @Override
    default CompletableFuture<Set<Event>> afterCreate(final ObjectSchema schema, final String id, final Map<String, Object> after) {

        final StorageTraits traits = storageTraits(schema);
        final List<Index> indexes = IndexStorage.getAsyncIndexes(traits, schema);
        if(!indexes.isEmpty()) {
            return DefaultLayerStorage.super.afterCreate(schema, id, after)
                    .thenCompose(events -> write(Consistency.ATOMIC, Versioning.CHECKED)
                        .createIndexes(schema, indexes, id, after)
                        .write().thenApply(ignored -> events));
        } else {
            return DefaultLayerStorage.super.afterCreate(schema, id, after);
        }
    }

    @Override
    default CompletableFuture<Set<Event>> afterUpdate(final ObjectSchema schema, final String id, final long version, final Map<String, Object> before, final Map<String, Object> after) {

        final StorageTraits traits = storageTraits(schema);
        final List<Index> indexes = IndexStorage.getAsyncIndexes(traits, schema);
        if(!indexes.isEmpty()) {
            return DefaultLayerStorage.super.afterUpdate(schema, id, version, before, after)
                    .thenCompose(events -> write(Consistency.ATOMIC, Versioning.CHECKED)
                            .updateIndexes(schema, indexes, id, before, after)
                            .write().thenApply(ignored -> events));
        } else {
            return DefaultLayerStorage.super.afterUpdate(schema, id, version, before, after);
        }
    }

    @Override
    default CompletableFuture<Set<Event>> afterDelete(final ObjectSchema schema, final String id, final long version, final Map<String, Object> before) {

        final StorageTraits traits = storageTraits(schema);
        final List<Index> indexes = IndexStorage.getAsyncIndexes(traits, schema);
        if(!indexes.isEmpty()) {
            return DefaultLayerStorage.super.afterDelete(schema, id, version, before)
                    .thenCompose(events -> write(Consistency.ATOMIC, Versioning.CHECKED)
                            .deleteIndexes(schema, indexes, id, before)
                            .write().thenApply(ignored -> events));
        } else {
            return DefaultLayerStorage.super.afterDelete(schema, id, version, before);
        }
    }

    Pager<Map<String, Object>> queryIndex(ObjectSchema schema, Index index, SatisfyResult satisfy, Map<Name, Range<Object>> query, List<Sort> sort, Set<Name> expand);

    @Override
    default Pager<Map<String, Object>> queryIndex(final ObjectSchema schema, final Index index, final Expression query, final List<Sort> sort, final Set<Name> expand) {

        final Map<Name, Range<Object>> ranges = new HashMap<>();
        for(final Map.Entry<Name, Range<Object>> entry : query.visit(new RangeVisitor()).entrySet()) {
            final Name name = entry.getKey();
            ranges.put(name, entry.getValue());
        }
        final Optional<SatisfyResult> optSatisfy = satisfy(index, ranges, sort);
        if (optSatisfy.isPresent()) {
            final SatisfyResult satisfy = optSatisfy.get();
            return queryIndex(schema, index, satisfy, ranges, sort, expand);
        } else {
            throw new UnsupportedQueryException(schema.getQualifiedName(), query, "index does not support this query");
        }
    }

    @Override
    default Pager<Map<String, Object>> queryObject(final Consistency consistency, final ObjectSchema schema, final Expression query, final List<Sort> sort, final Set<Name> expand) {

        final Expression bound = query.bind(Context.init());
        final Set<Expression> disjunction = bound.visit(new DisjunctionVisitor());

        final Map<String, Pager<Map<String, Object>>> pagers = new HashMap<>();

        List<Sort> indexSort = null;
        for (final Expression conjunction : disjunction) {
            final Map<Name, Range<Object>> ranges = new HashMap<>();
            for(final Map.Entry<Name, Range<Object>> entry : conjunction.visit(new RangeVisitor()).entrySet()) {
                final Name name = entry.getKey();
                ranges.put(name, entry.getValue());
            }

            final Optional<String> optId = constantId(ranges);
            if(optId.isPresent()) {

                pagers.put(conjunction.digest(), Pager.simple(get(Consistency.ATOMIC, schema, optId.get(), expand)
                        .thenApply(v -> v == null ? Collections.emptyList() : Collections.singletonList(v))));

            } else {
                final Optional<SatisfyResult> optSatisfy = satisfy(schema.getIndexes().values(), ranges, sort);
                if (optSatisfy.isPresent()) {

                    final SatisfyResult satisfy = optSatisfy.get();
                    final Index index = satisfy.getIndex();

                    // FIXME
                    if (indexSort == null) {
                        indexSort = index.getSort();
                    }

                    pagers.put(conjunction.digest(), queryIndex(schema, index, satisfy, ranges, sort, expand));

                } else {
                    throw new UnsupportedQueryException(schema.getQualifiedName(), query, "no index");
                }
            }
        }

        return Pager.merge(Instance.comparator(sort), pagers);
    }

    interface ReadTransaction extends IndexStorage.ReadTransaction, DefaultLayerStorage.ReadTransaction {

    }

    interface WriteTransaction extends IndexStorage.WriteTransaction, DefaultLayerStorage.WriteTransaction {

        void createIndex(ReferableSchema schema, Index index, String id, long version, Index.Key key, Map<String, Object> projection);

        void updateIndex(ReferableSchema schema, Index index, String id, long version, Index.Key key, Map<String, Object> projection);

        void deleteIndex(ReferableSchema schema, Index index, String id, long version, Index.Key key);

        @Override
        default Storage.WriteTransaction createObject(final ObjectSchema schema, final String id, final Map<String, Object> after) {

            final StorageTraits traits = storageTraits(schema);
            DefaultLayerStorage.WriteTransaction.super.createObject(schema, id, after);
            createIndexes(schema, IndexStorage.getSyncIndexes(traits, schema), id, after);
            return this;
        }

        @Override
        default Storage.WriteTransaction updateObject(final ObjectSchema schema, final String id, final Map<String, Object> before, final Map<String, Object> after) {

            final StorageTraits traits = storageTraits(schema);
            DefaultLayerStorage.WriteTransaction.super.updateObject(schema, id, before, after);
            updateIndexes(schema, IndexStorage.getSyncIndexes(traits, schema), id, before, after);
            return this;
        }

        @Override
        default Storage.WriteTransaction deleteObject(final ObjectSchema schema, final String id, final Map<String, Object> before) {

            final StorageTraits traits = storageTraits(schema);
            DefaultLayerStorage.WriteTransaction.super.deleteObject(schema, id, before);
            deleteIndexes(schema, IndexStorage.getSyncIndexes(traits, schema), id, before);
            return this;
        }

        @Override
        default IndexStorage.WriteTransaction createIndexes(final ReferableSchema schema, final List<Index> indexes, final String id, final Map<String, Object> after) {

            indexes.forEach(index -> {
                final Map<Index.Key, Map<String, Object>> records = index.readValues(after);
                records.forEach((key, projection) -> createIndex(schema, index, id, 0L, key, projection));
            });
            return this;
        }

        @Override
        default IndexStorage.WriteTransaction updateIndexes(final ReferableSchema schema, final List<Index> indexes, final String id, final Map<String, Object> before, final Map<String, Object> after) {

            if(before != null) {
                long version = Instance.getVersion(before);
                indexes.forEach(index -> {
                    final Index.Diff diff = Index.Diff.from(index.readValues(before), index.readValues(after));
                    diff.getCreate().forEach((key, projection) -> createIndex(schema, index, id, version, key, projection));
                    diff.getUpdate().forEach((key, projection) -> updateIndex(schema, index, id, version, key, projection));
                    diff.getDelete().forEach((key) -> deleteIndex(schema, index, id, version, key));
                });
            } else {
                indexes.forEach(index -> {
                    final Map<Index.Key, Map<String, Object>> records = index.readValues(after);
                    records.forEach((key, projection) -> createIndex(schema, index, id, 0L, key, projection));
                });
            }
            return this;
        }

        @Override
        default IndexStorage.WriteTransaction deleteIndexes(final ReferableSchema schema, final List<Index> indexes, final String id, final Map<String, Object> before) {

            if(before != null) {
                long version = Instance.getVersion(before);
                indexes.forEach(index -> {
                    final Map<Index.Key, Map<String, Object>> records = index.readValues(before);
                    records.keySet().forEach((key) -> deleteIndex(schema, index, id, version, key));
                });
            }
            return this;
        }
    }

    static Optional<SatisfyResult> satisfy(final Iterable<Index> indexes, final Map<Name, Range<Object>> query, final List<Sort> sort) {

        Optional<SatisfyResult> best = Optional.empty();
        for (final Index index : indexes) {
            final Optional<SatisfyResult> next = satisfy(index, query, sort);
            if(next.isPresent()) {
                final SatisfyResult a = next.get();
                if(best.isPresent()) {
                    final SatisfyResult b = best.get();
                    if(a.compareTo(b) > 0) {
                        best = next;
                    }
                } else {
                    best = next;
                }
            }
        }
        return best;
    }

    static Optional<String> constantId(final Map<Name, Range<Object>> query) {

        final Range<Object> range = query.get(Name.of(ObjectSchema.ID));
        if(range instanceof Range.Eq) {
            final Object eq = ((Range.Eq<?>) range).getEq();
            if(eq instanceof String) {
                return Optional.of((String)eq);
            }
        }
        return Optional.empty();
    }

    static Optional<SatisfyResult> satisfy(final Index index, final Map<Name, Range<Object>> query, final List<Sort> sort) {

        final Map<Name, Object> constants = new HashMap<>();
        query.forEach((k, v) -> {
            if(v instanceof Range.Eq) {
                constants.put(k, ((Range.Eq<?>) v).getEq());
            }
        });

        final List<Name> partition;
        final Map<String, Name> over = index.getOver();
        if(over.isEmpty()) {
            partition = index.getPartition();
        } else {
            partition = index.getPartition().stream()
                    .map(v -> {
                        final Name overName = over.get(v.first());
                        if(overName != null) {
                            return overName.with(v.withoutFirst());
                        } else {
                            return v;
                        }
                    })
                    .collect(Collectors.toList());
        }

        final Set<Name> matched = new HashSet<>();
        final List<Object> partitionValues = new ArrayList<>();
        for(final Name name : partition) {
            if(constants.containsKey(name)) {
                partitionValues.add(constants.get(name));
                matched.add(name);
            } else {
                return Optional.empty();
            }
        }
        final List<Object> sortValues = new ArrayList<>();
        for(final Sort s : index.getSort()) {
            final Name name = s.getName();
            if(constants.containsKey(name)) {
                sortValues.add(constants.get(name));
                matched.add(name);
            } else {
                break;
            }
        }
        int matchedSort = sortValues.size();
        boolean reversed = false;
        for(final Sort thatSort : sort) {
            if(!constants.containsKey(thatSort.getName())) {
                if (matchedSort < index.getSort().size()) {
                    final Sort thisSort = index.getSort().get(matchedSort);
                    if (thisSort.getName().equals(thatSort.getName())) {
                        final Sort.Order thisOrder = thisSort.getOrder();
                        if ((reversed ? thisOrder.reverse() : thisOrder).equals(thatSort.getOrder())) {
                            ++matchedSort;
                        } else if (!reversed) {
                            reversed = true;
                            ++matchedSort;
                        }
                    }
                }
            }
            // FIXME
            //return SatisfyResult.unsatisfied();
        }
        return Optional.of(new SatisfyResult(index, partitionValues, sortValues, reversed, matched));
    }

    @Data
    class SatisfyResult implements Comparable<SatisfyResult> {

        private final Index index;

        private final List<Object> partition;

        private final List<Object> sort;

        private final boolean reversed;

        private final Set<Name> matched;

        public boolean isMatched(final Name name) {

            return matched != null && matched.contains(name);
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
}

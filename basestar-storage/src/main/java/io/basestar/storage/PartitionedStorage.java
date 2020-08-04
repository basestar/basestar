package io.basestar.storage;

/*-
 * #%L
 * basestar-storage
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

import com.google.common.collect.ImmutableList;
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.expression.aggregate.Aggregate;
import io.basestar.schema.Consistency;
import io.basestar.schema.Index;
import io.basestar.schema.Instance;
import io.basestar.schema.ObjectSchema;
import io.basestar.storage.exception.UnsupportedQueryException;
import io.basestar.storage.query.DisjunctionVisitor;
import io.basestar.storage.query.Range;
import io.basestar.storage.query.RangeVisitor;
import io.basestar.storage.util.IndexRecordDiff;
import io.basestar.storage.util.Pager;
import io.basestar.util.Name;
import io.basestar.util.PagedList;
import io.basestar.util.PagingToken;
import io.basestar.util.Sort;
import lombok.Data;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public abstract class PartitionedStorage implements Storage.WithWriteIndex {

    protected abstract CompletableFuture<PagedList<Map<String, Object>>> queryIndex(ObjectSchema schema, Index index, SatisfyResult satisfyResult, Map<Name, Range<Object>> query, List<Sort> sort, Set<Name> expand, int count, PagingToken paging);

    @Override
    public List<Pager.Source<Map<String, Object>>> aggregate(final ObjectSchema schema, final Expression query, final Map<String, Expression> group, final Map<String, Aggregate> aggregates) {

        throw new UnsupportedOperationException();
    }

    @Override
    public List<Pager.Source<Map<String, Object>>> query(final ObjectSchema schema, final Expression expression, final List<Sort> sort, final Set<Name> expand) {

        final Expression bound = expression.bind(Context.init());
        final Set<Expression> disjunction = bound.visit(new DisjunctionVisitor());

        final List<Pager.Source<Map<String, Object>>> queries = new ArrayList<>();

        List<Sort> indexSort = null;
        for (final Expression conjunction : disjunction) {
            final Map<Name, Range<Object>> query = new HashMap<>();
            for(final Map.Entry<Name, Range<Object>> entry : conjunction.visit(new RangeVisitor()).entrySet()) {
                final Name name = entry.getKey();
//                if(path.isChild(Path.of(Reserved.THIS))) {
                    query.put(name, entry.getValue());
//                }
            }

            final Optional<String> optId = constantId(query);
            if(optId.isPresent()) {

                queries.add((c, p, stats) -> readObject(schema, optId.get(), expand).thenApply(object -> {
                    if(object != null) {
                        return new PagedList<>(ImmutableList.of(object), null);
                    } else {
                        return PagedList.<Map<String, Object>>empty();
                    }
                }));

            } else {
                final Optional<SatisfyResult> optSatisfy = satisfy(schema.getIndexes().values(), query, sort);
                if (optSatisfy.isPresent()) {

                    final SatisfyResult satisfy = optSatisfy.get();
                    final Index index = satisfy.getIndex();

                    // FIXME
                    if (indexSort == null) {
                        indexSort = index.getSort();
                    }

                    queries.add((c, p, stats) -> queryIndex(schema, index, satisfy, query, sort, expand, c, p));

                } else {
                    throw new UnsupportedQueryException(schema.getQualifiedName(), expression, "no index");
                }
            }
        }

        return queries;
    }

    public static Optional<SatisfyResult> satisfy(final Iterable<Index> indexes, final Map<Name, Range<Object>> query, final List<Sort> sort) {

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

    public static Optional<String> constantId(final Map<Name, Range<Object>> query) {

        final Range<Object> range = query.get(Name.of(ObjectSchema.ID));
        if(range instanceof Range.Eq) {
            final Object eq = ((Range.Eq<?>) range).getEq();
            if(eq instanceof String) {
                return Optional.of((String)eq);
            }
        }
        return Optional.empty();
    }

    public static Optional<SatisfyResult> satisfy(final Index index, final Map<Name, Range<Object>> query, final List<Sort> sort) {

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
    public static class SatisfyResult implements Comparable<SatisfyResult> {

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

    @Override
    public abstract WriteTransaction write(Consistency consistency, Versioning versioning);

    protected abstract class WriteTransaction implements WithWriteIndex.WriteTransaction {

        protected WriteTransaction createIndexes(final ObjectSchema schema, String id, Map<String, Object> after) {

            final StorageTraits traits = storageTraits(schema);
            schema.getIndexes().forEach((indexName, index) -> {
                final Consistency best = traits.getIndexConsistency(index.isMultiValue());
                if (!index.getConsistency(best).isAsync()) {
                    final Map<Index.Key, Map<String, Object>> records = index.readValues(after);
                    records.forEach((key, projection) -> createIndex(schema, index, id, 0L, key, projection));
                }
            });

            return this;
        }

        protected WriteTransaction updateIndexes(final ObjectSchema schema, final String id, final Map<String, Object> before, final Map<String, Object> after) {

            final Map<String, Index> indexes = schema.getIndexes();
            if (!indexes.isEmpty()) {
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

            final Map<String, Index> indexes = schema.getIndexes();
            if (!indexes.isEmpty()) {
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
    }
}

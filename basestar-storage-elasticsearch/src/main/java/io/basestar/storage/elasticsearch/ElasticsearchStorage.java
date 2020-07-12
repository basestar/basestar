package io.basestar.storage.elasticsearch;

/*-
 * #%L
 * basestar-storage-elasticsearch
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

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.expression.aggregate.Aggregate;
import io.basestar.schema.*;
import io.basestar.storage.BatchResponse;
import io.basestar.storage.Storage;
import io.basestar.storage.StorageTraits;
import io.basestar.storage.elasticsearch.mapping.Mappings;
import io.basestar.storage.elasticsearch.mapping.Settings;
import io.basestar.storage.exception.ObjectExistsException;
import io.basestar.storage.exception.VersionMismatchException;
import io.basestar.storage.util.KeysetPagingUtils;
import io.basestar.storage.util.Pager;
import io.basestar.util.Name;
import io.basestar.util.PagedList;
import io.basestar.util.PagingToken;
import io.basestar.util.Sort;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.*;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.HttpAsyncResponseConsumerFactory;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.function.Function;

@Slf4j
public class ElasticsearchStorage implements Storage.WithWriteHistory, Storage.WithoutWriteIndex {

    private static final String PRIMARY_TERM_KEY = "@primaryTerm";

    private static final String SEQ_NO_KEY = "@seqNo";

    private final RestHighLevelClient client;

    private final ElasticsearchStrategy strategy;

    private final EventStrategy eventStrategy;

    private final ConcurrentSkipListSet<String> createdIndices;

    // FIXME: make configurable
    private static final RequestOptions OPTIONS;

    static {
        final RequestOptions.Builder builder = RequestOptions.DEFAULT.toBuilder();
        builder.setHttpAsyncResponseConsumerFactory(
                new HttpAsyncResponseConsumerFactory
                        .HeapBufferedResponseConsumerFactory(10000000));
        OPTIONS = builder.build();
    }

    private ElasticsearchStorage(final Builder builder) {

        this.client = builder.client;
        this.strategy = builder.strategy;
        this.eventStrategy = MoreObjects.firstNonNull(builder.eventStrategy, EventStrategy.EMIT);
        this.createdIndices = new ConcurrentSkipListSet<>();
    }

    public static Builder builder() {

        return new Builder();
    }

    @Setter
    @Accessors(chain = true)
    public static class Builder {

        private RestHighLevelClient client;

        private ElasticsearchStrategy strategy;

        private EventStrategy eventStrategy;

        public ElasticsearchStorage build() {

            return new ElasticsearchStorage(this);
        }
    }

    @Override
    public CompletableFuture<Map<String, Object>> readObject(final ObjectSchema schema, final String id) {

        final String index = strategy.objectIndex(schema);
        return getIndex(index, schema).thenCompose(ignored -> {
            final GetRequest request = new GetRequest(index, id);
            return ElasticsearchUtils.<GetResponse>future(listener -> client.getAsync(request, OPTIONS, listener))
                    .thenApply(v -> fromResponse(schema, v));
        });
    }

    @Override
    public CompletableFuture<Map<String, Object>> readObjectVersion(final ObjectSchema schema, final String id, final long version) {

        if (!strategy.historyEnabled(schema)) {
            throw new UnsupportedOperationException("History not enabled");
        }
        final String index = strategy.historyIndex(schema);
        return getIndex(index, schema).thenCompose(ignored -> {
            final String key = historyKey(id, version);
            final GetRequest request = new GetRequest(index, key);
            return ElasticsearchUtils.<GetResponse>future(listener -> client.getAsync(request, OPTIONS, listener))
                    .thenApply(v -> fromResponse(schema, v));
        });
    }

    @Override
    public List<Pager.Source<Map<String, Object>>> query(final ObjectSchema schema, final Expression query, final List<Sort> sort) {

        final Expression bound = query.bind(Context.init());
        final String index = strategy.objectIndex(schema);

        final List<Sort> normalizedSort = KeysetPagingUtils.normalizeSort(sort);

        return ImmutableList.of(
                (count, token, stats) -> getIndex(index, schema).thenCompose(ignored -> {

                    final QueryBuilder queryBuilder = bound.visit(new ElasticsearchExpressionVisitor());

                    final QueryBuilder pagedQueryBuilder;
                    if (token == null) {
                        pagedQueryBuilder = queryBuilder;
                    } else if (queryBuilder == null) {
                        pagedQueryBuilder = pagingQueryBuilder(schema, normalizedSort, token);
                    } else {
                        pagedQueryBuilder = QueryBuilders.boolQuery()
                                .must(queryBuilder)
                                .must(pagingQueryBuilder(schema, normalizedSort, token));
                    }

                    final SearchRequest request = new SearchRequest(index)
                            .source(applySort(new SearchSourceBuilder()
                                    .query(pagedQueryBuilder), normalizedSort)
                                    .trackTotalHits(true));

                    return ElasticsearchUtils.<SearchResponse>future(listener -> client.searchAsync(request, OPTIONS, listener))
                            .thenApply(searchResponse -> {

                                final List<Map<String, Object>> results = new ArrayList<>();
                                Map<String, Object> last = null;
                                for (final SearchHit hit : searchResponse.getHits()) {
                                    last = fromHit(schema, hit);
                                    results.add(last);
                                }
                                final long total = searchResponse.getHits().getTotalHits().value;
                                final PagingToken newPaging;
                                if (total > results.size() && last != null) {
                                    newPaging = KeysetPagingUtils.keysetPagingToken(schema, normalizedSort, last);
                                } else {
                                    newPaging = null;
                                }
                                return new PagedList<>(results, newPaging);
                            });
                })
        );
    }

    @Override
    public List<Pager.Source<Map<String, Object>>> aggregate(final ObjectSchema schema, final Expression query, final Map<String, Expression> group, final Map<String, Aggregate> aggregates) {

        throw new UnsupportedOperationException();
    }

    private QueryBuilder pagingQueryBuilder(final ObjectSchema schema, final List<Sort> sort, final PagingToken token) {

        final List<Object> values = KeysetPagingUtils.keysetValues(schema, sort, token);
        final BoolQueryBuilder outer = QueryBuilders.boolQuery();
        for(int i = 0; i < sort.size(); ++i) {
            if(i == 0) {
                outer.should(pagingRange(sort.get(i), values.get(i)));
            } else {
                final BoolQueryBuilder inner = QueryBuilders.boolQuery();
                for (int j = 0; j < i; ++j) {
                    final Name name = sort.get(j).getName();
                    inner.must(QueryBuilders.termQuery(name.toString(), values.get(j)));
                }
                inner.must(pagingRange(sort.get(i), values.get(i)));
                outer.should(inner);
            }
        }
        return outer;
    }

    private QueryBuilder pagingRange(final Sort sort, final Object value) {

        final String name = sort.getName().toString();
        if(sort.getOrder() == Sort.Order.ASC) {
            return QueryBuilders.rangeQuery(name).gt(value);
        } else {
            return QueryBuilders.rangeQuery(name).lt(value);
        }
    }

    private SearchSourceBuilder applySort(final SearchSourceBuilder builder, final List<Sort> sort) {

        for (final Sort s : sort) {
            builder.sort(s.getName().toString(), s.getOrder() == Sort.Order.ASC ? SortOrder.ASC : SortOrder.DESC);
        }
        return builder;
    }

    @Override
    public ReadTransaction read(final Consistency consistency) {

        return new ReadTransaction() {

            private final MultiGetRequest request = new MultiGetRequest();

            private final Map<String, ObjectSchema> indexToSchema = new HashMap<>();

            @Override
            public ReadTransaction readObject(final ObjectSchema schema, final String id) {

                final String index = strategy.objectIndex(schema);
                indexToSchema.put(index, schema);
                request.add(index, id);
                return this;
            }

            @Override
            public ReadTransaction readObjectVersion(final ObjectSchema schema, final String id, final long version) {

                if (!strategy.historyEnabled(schema)) {
                    throw new UnsupportedOperationException("History not enabled");
                }
                final String index = strategy.historyIndex(schema);
                indexToSchema.put(index, schema);
                final String key = historyKey(id, version);
                request.add(index, key);
                return this;
            }

            @Override
            public CompletableFuture<BatchResponse> read() {

                return getIndices(indexToSchema).thenCompose(ignored -> ElasticsearchUtils.<MultiGetResponse>future(listener -> client.mgetAsync(request, OPTIONS, listener))
                        .thenApply(response -> {
                            final SortedMap<BatchResponse.Key, Map<String, Object>> results = new TreeMap<>();
                            for (final MultiGetItemResponse item : response) {
                                final String index = item.getIndex();
                                final ObjectSchema schema = indexToSchema.get(index);
                                final Map<String, Object> result = fromResponse(schema, item.getResponse());
                                if (result != null) {
                                    results.put(BatchResponse.Key.from(schema.getQualifiedName(), result), result);
                                }
                            }
                            return new BatchResponse.Basic(results);
                        }));
            }
        };
    }

    private Map<String, Object> fromHit(final ObjectSchema schema, final SearchHit hit) {

        return fromSource(schema, hit.getSourceAsMap());
    }

    private Map<String, Object> fromResponse(final ObjectSchema schema, final GetResponse item) {

        if (item.isExists()) {
            final Map<String, Object> result = new HashMap<>(fromSource(schema, item.getSourceAsMap()));
            result.put(PRIMARY_TERM_KEY, item.getPrimaryTerm());
            result.put(SEQ_NO_KEY, item.getSeqNo());
            return result;
        } else {
            return null;
        }
    }

    private Long getPrimaryTerm(final Map<String, Object> object) {

        return (Long) object.get(PRIMARY_TERM_KEY);
    }

    private Long getSeqNo(final Map<String, Object> object) {

        return (Long) object.get(SEQ_NO_KEY);
    }

    private CompletableFuture<?> getIndex(final String name, final ObjectSchema schema) {

        if (!createdIndices.contains(name)) {
            return syncIndex(name, schema);
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }

    private CompletableFuture<?> getIndices(final Map<String, ObjectSchema> indices) {

        final List<CompletableFuture<?>> createIndexFutures = new ArrayList<>();
        for (final Map.Entry<String, ObjectSchema> entry : indices.entrySet()) {
            if (!createdIndices.contains(entry.getKey())) {
                createIndexFutures.add(ElasticsearchStorage.this.syncIndex(entry.getKey(), entry.getValue()));
            }
        }
        return CompletableFuture.allOf(createIndexFutures.toArray(new CompletableFuture<?>[0]));
    }

    private CompletableFuture<?> syncIndex(final String name, final ObjectSchema schema) {

        final Mappings mappings = strategy.mappings(schema);
        final Settings settings = strategy.settings(schema);
        return ElasticsearchUtils.syncIndex(client, name, mappings, settings);
    }

    @Override
    public WriteTransaction write(final Consistency consistency) {

        return new WriteTransaction();
    }

    protected class WriteTransaction implements WithWriteHistory.WriteTransaction {

        private final WriteRequest.RefreshPolicy refreshPolicy = WriteRequest.RefreshPolicy.WAIT_UNTIL;

        final BulkRequest request = new BulkRequest()
                .setRefreshPolicy(refreshPolicy);

        final List<Function<BulkItemResponse, BatchResponse>> responders = new ArrayList<>();

        final Map<String, ObjectSchema> indices = new HashMap<>();

        @Override
        public Storage.WriteTransaction createObject(final ObjectSchema schema, final String id, final Map<String, Object> after) {

            final String index = strategy.objectIndex(schema);
            indices.put(index, schema);
            request.add(new IndexRequest()
                    .index(index).source(toSource(schema, after)).id(id)
                    .opType(DocWriteRequest.OpType.CREATE));
            responders.add((response) -> {
                final BulkItemResponse.Failure failure = response.getFailure();
                if (failure != null && failure.getStatus() == RestStatus.CONFLICT) {
                    throw new ObjectExistsException(schema.getQualifiedName(), id);
                }
                return BatchResponse.single(schema.getQualifiedName(), after);
            });

            checkAndCreateHistory(schema, id, after);

            return this;
        }

        private void checkAndCreateHistory(final ObjectSchema schema, final String id, final Map<String, Object> after) {

            final History history = schema.getHistory();
            if (history.isEnabled() && history.getConsistency(Consistency.ATOMIC).isStronger(Consistency.ASYNC)) {
                final Long afterVersion = Instance.getVersion(after);
                assert afterVersion != null;
                createHistory(schema, id, afterVersion, after);
            }
        }

        @Override
        public Storage.WriteTransaction updateObject(final ObjectSchema schema, final String id, final Map<String, Object> before, final Map<String, Object> after) {

            final String index = strategy.objectIndex(schema);
            indices.put(index, schema);

            final IndexRequest req = new IndexRequest().id(id)
                    .index(index).source(toSource(schema, after));

            final Long version;
            if (before != null) {
                version = Instance.getVersion(before);
                if (version != null) {
                    final Long primaryTerm = getPrimaryTerm(before);
                    final Long seqNo = getSeqNo(before);
                    // FIXME: should be required
                    if(primaryTerm != null && seqNo != null) {
                        req.setIfSeqNo(seqNo);
                        req.setIfPrimaryTerm(primaryTerm);
                    }
                }
            } else {
                version = null;
            }

            request.add(req);
            responders.add(response -> {
                final BulkItemResponse.Failure failure = response.getFailure();
                if (failure != null && failure.getStatus() == RestStatus.CONFLICT) {
                    assert version != null;
                    throw new VersionMismatchException(schema.getQualifiedName(), id, version);
                }
                return BatchResponse.single(schema.getQualifiedName(), after);
            });

            checkAndCreateHistory(schema, id, after);

            return this;
        }

        @Override
        public Storage.WriteTransaction deleteObject(final ObjectSchema schema, final String id, final Map<String, Object> before) {

            final String index = strategy.objectIndex(schema);
            indices.put(index, schema);

            final DeleteRequest req = new DeleteRequest().id(id)
                    .index(index);

            final Long version;
            if (before != null) {
                version = Instance.getVersion(before);
                if (version != null) {
                    final Long primaryTerm = getPrimaryTerm(before);
                    final Long seqNo = getSeqNo(before);
                    // FIXME: should be required
                    if(primaryTerm != null && seqNo != null) {
                        req.setIfSeqNo(seqNo);
                        req.setIfPrimaryTerm(primaryTerm);
                    }
                }
            } else {
                version = null;
            }

            request.add(req);
            responders.add(response -> {
                final BulkItemResponse.Failure failure = response.getFailure();
                if (failure != null && failure.getStatus() == RestStatus.CONFLICT) {
                    throw new VersionMismatchException(schema.getQualifiedName(), id, version);
                }
                return BatchResponse.empty();
            });

            return this;
        }

        @Override
        public WriteTransaction createHistory(final ObjectSchema schema, final String id, final long version, final Map<String, Object> after) {

            if (strategy.historyEnabled(schema)) {
                final String index = strategy.historyIndex(schema);
                final String key = historyKey(id, version);
                indices.put(index, schema);
                request.add(new IndexRequest()
                        .index(index).source(toSource(schema, after)).id(key)
                        .opType(DocWriteRequest.OpType.CREATE));
                responders.add(response -> BatchResponse.single(schema.getQualifiedName(), after));
            }
            return this;
        }

        @Override
        public CompletableFuture<BatchResponse> write() {

            return getIndices(indices)
                    .thenCompose(ignored -> ElasticsearchUtils
                            .<BulkResponse>future(listener -> client.bulkAsync(request, OPTIONS, listener))
                            .thenApply(response -> {
                                final SortedMap<BatchResponse.Key, Map<String, Object>> results = new TreeMap<>();
                                final BulkItemResponse[] items = response.getItems();
                                assert (items.length == responders.size());
                                for (int i = 0; i != items.length; ++i) {
                                    results.putAll(responders.get(i).apply(items[i]));
                                }
                                return new BatchResponse.Basic(results);
                            }));
        }
    }

    private Map<String, Object> toSource(final ObjectSchema schema, final Map<String, Object> data) {

        final Map<String, Object> source = new HashMap<>();
        final Mappings mappings = strategy.mappings(schema);
        mappings.getProperties().forEach((name, type) -> {
            final Object value = data.get(name);
            source.put(name, type.toSource(value));
        });
        return source;
    }

    private Map<String, Object> fromSource(final ObjectSchema schema, final Map<String, Object> source) {

        final Map<String, Object> data = new HashMap<>();
        final Mappings mappings = strategy.mappings(schema);
        mappings.getProperties().forEach((name, type) -> {
            final Object value = source.get(name);
            data.put(name, type.fromSource(value));
        });
        return data;
    }

    @Override
    public EventStrategy eventStrategy(final ObjectSchema schema) {

        return eventStrategy;
    }

    @Override
    public StorageTraits storageTraits(final ObjectSchema schema) {

        return ElasticsearchStorageTraits.INSTANCE;
    }

    private static String historyKey(final String id, final long version) {

        return id + Reserved.DELIMITER + version;
    }
}

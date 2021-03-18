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

import io.basestar.expression.Expression;
import io.basestar.schema.*;
import io.basestar.storage.*;
import io.basestar.storage.elasticsearch.expression.ESExpressionVisitor;
import io.basestar.storage.elasticsearch.mapping.Mappings;
import io.basestar.storage.elasticsearch.mapping.Settings;
import io.basestar.storage.elasticsearch.query.ESQueryStage;
import io.basestar.storage.elasticsearch.query.ESQueryStageVisitor;
import io.basestar.storage.exception.ObjectExistsException;
import io.basestar.storage.exception.VersionMismatchException;
import io.basestar.storage.query.QueryPlanner;
import io.basestar.util.*;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.HttpAsyncResponseConsumerFactory;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.function.Function;

@Slf4j
public class ElasticsearchStorage implements DefaultLayerStorage {

    private static final String PRIMARY_TERM_KEY = "@primaryTerm";

    private static final String SEQ_NO_KEY = "@seqNo";

    private static final int SCROLL_SIZE = 100;

    private static final long SCROLL_KEEPALIVE_MINUTES = 5;

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
        this.eventStrategy = Nullsafe.orDefault(builder.eventStrategy, EventStrategy.EMIT);
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
    public Pager<Map<String, Object>> query(final Consistency consistency, final LinkableSchema schema, final Expression query, final List<Sort> sort, final Set<Name> expand) {

        final QueryPlanner<ESQueryStage> planner = new QueryPlanner.Default<>(false);
        final ESQueryStage stage = planner.plan(new ESQueryStageVisitor(strategy), schema, query, sort, expand);

        final Mappings mappings = strategy.mappings(schema);

        return (stats, token, count) -> {
            final CompletableFuture<?> refreshFuture;
            final Set<String> indices = stage.indices();
            if(consistency.isStrongerOrEqual(Consistency.QUORUM) && !indices.isEmpty()) {
                final RefreshRequest refresh = new RefreshRequest(indices.toArray(new String[0]))
                        .indicesOptions(IndicesOptions.lenientExpandOpen());
                refreshFuture = ElasticsearchUtils.<RefreshResponse>future(listener -> client.indices().refreshAsync(refresh, OPTIONS, listener));
            } else {
                refreshFuture = CompletableFuture.completedFuture(null);
            }
            return refreshFuture.thenCompose(ignored -> stage.request(stats, token, count)
                    .map(request -> {
                        log.debug("Search request is {}", request);
                        return ElasticsearchUtils.<SearchResponse>future(listener -> client.searchAsync(request, OPTIONS, listener))
                                .thenApply(response -> {
                                    log.debug("Search response is {}", response);
                                    return stage.page(mappings, stats, token, count, response)
                                            .map(v -> (Map<String, Object>)schema.create(v, expand, true));
                                });

                    }).orElse(CompletableFuture.completedFuture(Page.empty())));
        };
    }

    @Override
    public Pager<Map<String, Object>> queryObject(final Consistency consistency, final ObjectSchema schema, final Expression query, final List<Sort> sort, final Set<Name> expand) {

        return query(consistency, schema, query, sort, expand);
    }

    @Override
    public Pager<Map<String, Object>> queryView(final Consistency consistency, final ViewSchema schema, final Expression query, final List<Sort> sort, final Set<Name> expand) {

        return query(consistency, schema, query, sort, expand);
    }

    @Override
    public ReadTransaction read(final Consistency consistency) {

        final boolean refresh = consistency.isStrongerOrEqual(Consistency.QUORUM);
        return new ReadTransaction() {

            private final MultiGetRequest request = new MultiGetRequest()
                            .refresh(refresh);

            private final Map<String, ReferableSchema> indexToSchema = new HashMap<>();

            @Override
            public ReadTransaction getObject(final ObjectSchema schema, final String id, final Set<Name> expand) {

                final String index = strategy.index(schema);
                indexToSchema.put(index, schema);
                request.add(index, id);
                return this;
            }

            @Override
            public ReadTransaction getObjectVersion(final ObjectSchema schema, final String id, final long version, final Set<Name> expand) {

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

                if(request.getItems().size() == 0) {
                    return CompletableFuture.completedFuture(BatchResponse.empty());
                } else {
                    return getIndices(indexToSchema).thenCompose(ignored -> ElasticsearchUtils.<MultiGetResponse>future(listener -> client.mgetAsync(request, OPTIONS, listener))
                            .thenApply(response -> {
                                final SortedMap<BatchResponse.RefKey, Map<String, Object>> results = new TreeMap<>();
                                for (final MultiGetItemResponse item : response) {
                                    final String index = item.getIndex();
                                    final ReferableSchema schema = indexToSchema.get(index);
                                    final Map<String, Object> result = fromResponse(schema, item.getResponse());
                                    if (result != null) {
                                        results.put(BatchResponse.RefKey.from(schema.getQualifiedName(), result), result);
                                    }
                                }
                                return BatchResponse.fromRefs(results);
                            }));
                }
            }
        };
    }

    private Map<String, Object> fromHit(final ReferableSchema schema, final SearchHit hit) {

        return fromSource(schema, hit.getSourceAsMap());
    }

    private Map<String, Object> fromResponse(final ReferableSchema schema, final GetResponse item) {

        if (item.isExists()) {
            final Map<String, Object> result = new HashMap<>(fromSource(schema, item.getSourceAsMap()));
            result.put(Reserved.META, new ElasticsearchMetadata(item.getPrimaryTerm(), item.getSeqNo()));
            return result;
        } else {
            return null;
        }
    }

    private CompletableFuture<?> getIndex(final String name, final ObjectSchema schema) {

        if (!createdIndices.contains(name)) {
            createdIndices.add(name);
            return syncIndex(name, schema);
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }

    private CompletableFuture<?> getIndices(final Map<String, ? extends LinkableSchema> indices) {

        final List<CompletableFuture<?>> createIndexFutures = new ArrayList<>();
        for (final Map.Entry<String, ? extends LinkableSchema> entry : indices.entrySet()) {
            if (!createdIndices.contains(entry.getKey())) {
                createdIndices.add(entry.getKey());
                createIndexFutures.add(ElasticsearchStorage.this.syncIndex(entry.getKey(), entry.getValue()));
            }
        }
        return CompletableFuture.allOf(createIndexFutures.toArray(new CompletableFuture<?>[0]));
    }

    private CompletableFuture<?> syncIndex(final String name, final LinkableSchema schema) {

        final Mappings mappings = strategy.mappings(schema);
        final Settings settings = strategy.settings(schema);
        return ElasticsearchUtils.syncIndex(client, name, mappings, settings);
    }

    @Override
    public WriteTransaction write(final Consistency consistency, final Versioning versioning) {

        return new WriteTransaction(consistency, versioning);
    }

    protected class WriteTransaction implements DefaultLayerStorage.WriteTransaction {

        private final WriteRequest.RefreshPolicy refreshPolicy;

        private final Versioning versioning;

        private final BulkRequest request;

        private final List<Function<BulkItemResponse, BatchResponse>> responders = new ArrayList<>();

        private final Map<String, LinkableSchema> indices = new HashMap<>();

        public WriteTransaction(final Consistency consistency, final Versioning versioning) {

            if(consistency.isStrongerOrEqual(Consistency.QUORUM)) {
                this.refreshPolicy = WriteRequest.RefreshPolicy.WAIT_UNTIL;
            } else {
                this.refreshPolicy = WriteRequest.RefreshPolicy.NONE;
            }
            this.versioning = versioning;
            this.request = new BulkRequest()
                    .setRefreshPolicy(refreshPolicy);
        }

        @Override
        public StorageTraits storageTraits(final ReferableSchema schema) {

            return ElasticsearchStorage.this.storageTraits(schema);
        }

        @Override
        public void createObjectLayer(final ReferableSchema schema, final String id, final Map<String, Object> after) {

            final String index = strategy.index(schema);
            indices.put(index, schema);
            request.add(new IndexRequest()
                    .index(index).source(toSource(schema, after)).id(id)
                    .opType(DocWriteRequest.OpType.CREATE));
            responders.add((response) -> {
                final BulkItemResponse.Failure failure = response.getFailure();
                if (failure != null && failure.getStatus() == RestStatus.CONFLICT) {
                    throw new ObjectExistsException(schema.getQualifiedName(), id);
                }
                return BatchResponse.fromRef(schema.getQualifiedName(), after);
            });
        }

        private Long version(final Map<String, Object> before, final DocWriteRequest<?> req) {

            final Long version;
            if (before != null) {
                version = Instance.getVersion(before);
                if (version != null) {
                    final ElasticsearchMetadata meta = Metadata.readFrom(before, ElasticsearchMetadata.class);
                    // FIXME: should be read if not present
                    if(meta != null) {
                        req.setIfSeqNo(meta.getSeqNo());
                        req.setIfPrimaryTerm(meta.getPrimaryTerm());
                    } else {
                        log.warn("No seqNo/primaryTerm in before state");
                    }
                }
            } else {
                version = null;
            }
            return version;
        }

        @Override
        public void updateObjectLayer(final ReferableSchema schema, final String id, final Map<String, Object> before, final Map<String, Object> after) {

            final String index = strategy.index(schema);
            indices.put(index, schema);

            final IndexRequest req = new IndexRequest().id(id)
                    .index(index).source(toSource(schema, after));

            final Long version = version(before, req);

            request.add(req);
            responders.add(response -> {
                final BulkItemResponse.Failure failure = response.getFailure();
                if (failure != null && failure.getStatus() == RestStatus.CONFLICT) {
                    assert version != null;
                    throw new VersionMismatchException(schema.getQualifiedName(), id, version);
                }
                return BatchResponse.fromRef(schema.getQualifiedName(), after);
            });
        }

        @Override
        public void deleteObjectLayer(final ReferableSchema schema, final String id, final Map<String, Object> before) {

            final String index = strategy.index(schema);
            indices.put(index, schema);

            final DeleteRequest req = new DeleteRequest().id(id)
                    .index(index);

            final Long version = version(before, req);

            request.add(req);
            responders.add(response -> {
                final BulkItemResponse.Failure failure = response.getFailure();
                if (failure != null && failure.getStatus() == RestStatus.CONFLICT) {
                    throw new VersionMismatchException(schema.getQualifiedName(), id, version);
                }
                return BatchResponse.empty();
            });
        }

        @Override
        public void writeHistoryLayer(final ReferableSchema schema, final String id, final Map<String, Object> after) {

            if (strategy.historyEnabled(schema)) {
                final long version = Instance.getVersion(after);
                final String index = strategy.historyIndex(schema);
                final String key = historyKey(id, version);
                indices.put(index, schema);
                request.add(new IndexRequest()
                        .index(index).source(toSource(schema, after)).id(key)
                        .opType(DocWriteRequest.OpType.CREATE));
                responders.add(response -> BatchResponse.fromRef(schema.getQualifiedName(), after));
            }
        }

        @Override
        public WriteTransaction write(final LinkableSchema schema, final Map<String, Object> after) {

            final String index = strategy.index(schema);
            final String id = schema.id(after);
            indices.put(index, schema);
            request.add(new IndexRequest()
                    .index(index).source(toSource(schema, after)).id(id)
                    .opType(DocWriteRequest.OpType.INDEX));
            responders.add(response -> BatchResponse.empty());
            return this;
        }

        @Override
        public CompletableFuture<BatchResponse> write() {

            if(request.numberOfActions() == 0) {
                return CompletableFuture.completedFuture(BatchResponse.empty());
            } else {
                return getIndices(indices)
                        .thenCompose(ignored -> ElasticsearchUtils
                                .<BulkResponse>future(listener -> client.bulkAsync(request, OPTIONS, listener))
                                .thenApply(response -> {
                                    final SortedMap<BatchResponse.RefKey, Map<String, Object>> results = new TreeMap<>();
                                    final BulkItemResponse[] items = response.getItems();
                                    assert (items.length == responders.size());
                                    for (int i = 0; i != items.length; ++i) {
                                        results.putAll(responders.get(i).apply(items[i]).getRefs());
                                    }
                                    return BatchResponse.fromRefs(results);
                                }));
            }
        }
    }

    private Map<String, Object> toSource(final LinkableSchema schema, final Map<String, Object> data) {

        final Mappings mappings = strategy.mappings(schema);
        return mappings.toSource(data);
    }

    private Map<String, Object> fromSource(final LinkableSchema schema, final Map<String, Object> source) {

        final Mappings mappings = strategy.mappings(schema);
        return mappings.fromSource(source);
    }

    @Override
    public EventStrategy eventStrategy(final ReferableSchema schema) {

        return eventStrategy;
    }

    @Override
    public StorageTraits storageTraits(final ReferableSchema schema) {

        return ElasticsearchStorageTraits.INSTANCE;
    }

    private static String historyKey(final String id, final long version) {

        return id + Reserved.DELIMITER + version;
    }

    @Override
    public Scan scan(final ReferableSchema schema, final Expression query, final int segments) {


        return new Scan() {
            @Override
            public int getSegments() {

                return segments;
            }

            @Override
            public Segment segment(final int segment) {

                return null;
            }
        };
    }

    private class ScanSegment implements Scan.Segment {


        private final ReferableSchema schema;

        private final Expression filter;

        private final int segments;

        private final int segment;

        private LinkedList<SearchHit> hits;

        private String scrollId;

        public ScanSegment(final ReferableSchema schema, final Expression filter, final int segments, final int segment) {

            this.schema = schema;
            this.filter = filter;
            this.segments = segments;
            this.segment = segment;
        }

        @Override
        public void close() {

            // not required
        }

        private void prepare() {

            try {
                if(hits == null || scrollId != null) {
                    final SearchResponse response;
                    if (scrollId == null) {
                        final SearchSourceBuilder src = new SearchSourceBuilder();
                        src.query(new ESExpressionVisitor().visit(filter));
                        src.size(SCROLL_SIZE);

                        final SearchRequest request = new SearchRequest(strategy.index(schema));
                        request.scroll(TimeValue.timeValueMinutes(SCROLL_KEEPALIVE_MINUTES));
                        request.source(src);
                        response = client.search(request, OPTIONS);
                    } else {
                        final SearchScrollRequest request = new SearchScrollRequest(scrollId);
                        request.scroll(TimeValue.timeValueMinutes(SCROLL_KEEPALIVE_MINUTES));
                        response = client.scroll(request, OPTIONS);
                    }
                    final SearchHit[] hits = response.getHits().getHits();
                    this.hits = new LinkedList<>(Arrays.asList(hits));
                    this.scrollId = hits.length == 0 ? null : response.getScrollId();
                }
            } catch (final IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public boolean hasNext() {

            prepare();
            return !hits.isEmpty();
        }

        @Override
        public Map<String, Object> next() {

            prepare();
            if(hits.isEmpty()) {
                throw new NoSuchElementException();
            }
            final Map<String, Object> result = fromSource(schema, hits.getFirst().getSourceAsMap());
            hits.pop();
            return result;
        }
    }
}

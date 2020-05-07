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
import io.basestar.schema.*;
import io.basestar.storage.BatchResponse;
import io.basestar.storage.Storage;
import io.basestar.storage.StorageTraits;
import io.basestar.storage.elasticsearch.mapping.Mappings;
import io.basestar.storage.elasticsearch.mapping.Settings;
import io.basestar.storage.exception.ObjectExistsException;
import io.basestar.storage.exception.VersionMismatchException;
import io.basestar.storage.util.Pager;
import io.basestar.util.PagedList;
import io.basestar.util.Sort;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.ActionListener;
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
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.function.Function;

@Slf4j
public class ElasticsearchStorage implements Storage {

    private static final String PRIMARY_TERM_KEY = "@primaryTerm";

    private static final String SEQ_NO_KEY = "@seqNo";

    private final RestHighLevelClient client;

    private final ElasticsearchRouting routing;

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
        this.routing = builder.routing;
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

        private ElasticsearchRouting routing;

        private EventStrategy eventStrategy;

        public ElasticsearchStorage build() {

            return new ElasticsearchStorage(this);
        }
    }

    @Override
    public CompletableFuture<Map<String, Object>> readObject(final ObjectSchema schema, final String id) {

        final String index = routing.objectIndex(schema);
        final GetRequest request = new GetRequest(index, id);
        return ElasticsearchStorage.<GetResponse>future(listener -> client.getAsync(request, OPTIONS, listener))
                .thenApply(v -> fromResponse(schema, v));
    }

    @Override
    public CompletableFuture<Map<String, Object>> readObjectVersion(final ObjectSchema schema, final String id, final long version) {

        if(!routing.historyEnabled(schema)) {
            throw new UnsupportedOperationException("History not enabled");
        }
        final String index = routing.historyIndex(schema);
        final String key = historyKey(id, version);
        final GetRequest request = new GetRequest(index, key);
        return ElasticsearchStorage.<GetResponse>future(listener -> client.getAsync(request, OPTIONS, listener))
                .thenApply(v -> fromResponse(schema, v));
    }

    @Override
    public List<Pager.Source<Map<String, Object>>> query(final ObjectSchema schema, final Expression query, final List<Sort> sort) {

        final Expression bound = query.bind(Context.init());
        final String index = routing.objectIndex(schema);

        final QueryBuilder queryBuilder = bound.visit(new ElasticsearchExpressionVisitor());

        assert queryBuilder != null;

        final SearchRequest request = new SearchRequest(index)
                .source(new SearchSourceBuilder()
                        .query(queryBuilder));

        return ImmutableList.of(
                (count, token) -> ElasticsearchStorage.<SearchResponse>future(listener -> client.searchAsync(request, OPTIONS, listener))
                        .thenApply(searchResponse -> {

                            final List<Map<String, Object>> results = new ArrayList<>();
                            for(final SearchHit hit : searchResponse.getHits()) {
                                results.add(fromHit(schema, hit));
                            }
                            return new PagedList<>(results, null);
                        })
        );
    }

    @Override
    public ReadTransaction read(final Consistency consistency) {

        return new ReadTransaction() {

            private final MultiGetRequest request = new MultiGetRequest();

            private final Map<String, ObjectSchema> indexToSchema = new HashMap<>();

            @Override
            public ReadTransaction readObject(final ObjectSchema schema, final String id) {

                final String index = routing.objectIndex(schema);
                indexToSchema.put(index, schema);
                request.add(index, id);
                return this;
            }

            @Override
            public ReadTransaction readObjectVersion(final ObjectSchema schema, final String id, final long version) {

                if(!routing.historyEnabled(schema)) {
                    throw new UnsupportedOperationException("History not enabled");
                }
                final String index = routing.historyIndex(schema);
                indexToSchema.put(index, schema);
                final String key = historyKey(id, version);
                request.add(index, key);
                return this;
            }

            @Override
            public CompletableFuture<BatchResponse> read() {

                return ElasticsearchStorage.<MultiGetResponse>future(listener -> client.mgetAsync(request, OPTIONS, listener))
                        .thenApply(response -> {
                            final SortedMap<BatchResponse.Key, Map<String, Object>> results = new TreeMap<>();
                            for(final MultiGetItemResponse item : response) {
                                final String index = item.getIndex();
                                final ObjectSchema schema = indexToSchema.get(index);
                                final Map<String, Object> result = fromResponse(schema, item.getResponse());
                                if (result != null) {
                                    results.put(BatchResponse.Key.from(schema.getName(), result), result);
                                }
                            }
                            return new BatchResponse.Basic(results);
                        });
            }
        };
    }

    private Map<String, Object> fromHit(final ObjectSchema schema, final SearchHit hit) {

        return fromSource(schema, hit.getSourceAsMap());
    }

    private Map<String, Object> fromResponse(final ObjectSchema schema, final GetResponse item) {

        if(item.isExists()) {
            final Map<String, Object> result = new HashMap<>(fromSource(schema, item.getSourceAsMap()));
            result.put(PRIMARY_TERM_KEY, item.getPrimaryTerm());
            result.put(SEQ_NO_KEY, item.getSeqNo());
            return result;
        } else {
            return null;
        }
    }

    private Long getPrimaryTerm(final Map<String, Object> object) {

        return (Long)object.get(PRIMARY_TERM_KEY);
    }

    private Long getSeqNo(final Map<String, Object> object) {

        return (Long)object.get(SEQ_NO_KEY);
    }

    private CompletableFuture<?> createIndex(final String name, final ObjectSchema schema) {

        final Mappings mappings = routing.mappings(schema);
        final Settings settings = routing.settings(schema);
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder()
                    .prettyPrint().startObject();
//            builder.field("include_type_name", false);
            builder = builder.startObject("settings");
            builder = settings.build(builder);
            builder = builder.endObject();
            builder = builder.startObject("mappings");
            builder = mappings.build(builder);
            builder = builder.endObject();
            builder = builder.endObject();

            final XContentBuilder source = builder;
            return ElasticsearchStorage.<CreateIndexResponse>future(listener ->
                    client.indices().createAsync(new CreateIndexRequest(name)
                            .source(source), OPTIONS, listener))
                    .exceptionally(e -> {
                        // FIXME?
                        log.error("Error creating search index, ignoring", e);
                        return null;
                    })
                    .thenApply(ignored -> createdIndices.add(name));

        } catch (final IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public WriteTransaction write(final Consistency consistency) {

        return new WriteTransaction() {

            private final WriteRequest.RefreshPolicy refreshPolicy = WriteRequest.RefreshPolicy.WAIT_UNTIL;

            final BulkRequest request = new BulkRequest()
                    .setRefreshPolicy(refreshPolicy);

            final List<Function<BulkItemResponse, BatchResponse>> responders = new ArrayList<>();

            final Map<String, ObjectSchema> indices = new HashMap<>();

            @Override
            public WriteTransaction createObject(final ObjectSchema schema, final String id, final Map<String, Object> after) {

                final String index = routing.objectIndex(schema);
                indices.put(index, schema);
                request.add(new IndexRequest()
                        .index(index).source(toSource(schema, after)).id(id)
                        .opType(DocWriteRequest.OpType.CREATE));
                responders.add((response) -> {
                    final BulkItemResponse.Failure failure = response.getFailure();
                    if(failure != null && failure.getStatus() == RestStatus.CONFLICT) {
                        throw new ObjectExistsException(schema.getName(), id);
                    }
                    return BatchResponse.single(schema.getName(), after);
                });

                checkAndCreateHistory(schema, id, after);

                return this;
            }

            private void checkAndCreateHistory(final ObjectSchema schema, final String id, final Map<String, Object> after) {

                final History history = schema.getHistory();
                if(history.isEnabled() && history.getConsistency(Consistency.ATOMIC).isStronger(Consistency.ASYNC)) {
                    final Long afterVersion = Instance.getVersion(after);
                    assert afterVersion != null;
                    createHistory(schema, id, afterVersion, after);
                }
            }

            @Override
            public WriteTransaction updateObject(final ObjectSchema schema, final String id, final Map<String, Object> before, final Map<String, Object> after) {

                final String index = routing.objectIndex(schema);
                indices.put(index, schema);

                final IndexRequest req = new IndexRequest().id(id)
                        .index(index).source(toSource(schema, after));

                final Long version;
                if(before != null) {
                    version = Instance.getVersion(before);
                    if(version != null) {
                        final Long primaryTerm = getPrimaryTerm(before);
                        final Long seqNo = getSeqNo(before);
                        assert primaryTerm != null && seqNo != null;
                        req.setIfSeqNo(seqNo);
                        req.setIfPrimaryTerm(primaryTerm);
                    }
                } else {
                    version = null;
                }

                request.add(req);
                responders.add(response -> {
                    final BulkItemResponse.Failure failure = response.getFailure();
                    if(failure != null && failure.getStatus() == RestStatus.CONFLICT) {
                        assert version != null;
                        throw new VersionMismatchException(schema.getName(), id, version);
                    }
                    return BatchResponse.single(schema.getName(), after);
                });

                checkAndCreateHistory(schema, id, after);

                return this;
            }

            @Override
            public WriteTransaction deleteObject(final ObjectSchema schema, final String id, final Map<String, Object> before) {

                final String index = routing.objectIndex(schema);
                indices.put(index, schema);

                final DeleteRequest req = new DeleteRequest().id(id)
                        .index(index);

                final Long version;
                if(before != null) {
                    version = Instance.getVersion(before);
                    if(version != null) {
                        final Long primaryTerm = getPrimaryTerm(before);
                        final Long seqNo = getSeqNo(before);
                        assert primaryTerm != null && seqNo != null;
                        req.setIfSeqNo(seqNo);
                        req.setIfPrimaryTerm(primaryTerm);
                    }
                } else {
                    version = null;
                }

                request.add(req);
                responders.add(response -> {
                    final BulkItemResponse.Failure failure = response.getFailure();
                    if(failure != null && failure.getStatus() == RestStatus.CONFLICT) {
                        throw new VersionMismatchException(schema.getName(), id, version);
                    }
                    return BatchResponse.empty();
                });

                return this;
            }

            @Override
            public WriteTransaction createIndex(final ObjectSchema schema, final Index index, final String id, final long version, final Index.Key key, final Map<String, Object> projection) {

                throw new UnsupportedOperationException();
            }

            @Override
            public WriteTransaction updateIndex(final ObjectSchema schema, final Index index, final String id, final long version, final Index.Key key, final Map<String, Object> projection) {

                throw new UnsupportedOperationException();
            }

            @Override
            public WriteTransaction deleteIndex(final ObjectSchema schema, final Index index, final String id, final long version, final Index.Key key) {

                throw new UnsupportedOperationException();
            }

            @Override
            public WriteTransaction createHistory(final ObjectSchema schema, final String id, final long version, final Map<String, Object> after) {

                if(routing.historyEnabled(schema)) {
                    final String index = routing.historyIndex(schema);
                    final String key = historyKey(id, version);
                    indices.put(index, schema);
                    request.add(new IndexRequest()
                            .index(index).source(toSource(schema, after)).id(key)
                            .opType(DocWriteRequest.OpType.CREATE));
                    responders.add(response -> BatchResponse.single(schema.getName(), after));
                }
                return this;
            }

            @Override
            public CompletableFuture<BatchResponse> commit() {

                final List<CompletableFuture<?>> createIndexFutures = new ArrayList<>();
                for(final Map.Entry<String, ObjectSchema> entry : indices.entrySet()) {
                    if(!createdIndices.contains(entry.getKey())) {
                        createIndexFutures.add(ElasticsearchStorage.this.createIndex(entry.getKey(), entry.getValue()));
                    }
                }
                return CompletableFuture.allOf(createIndexFutures.toArray(new CompletableFuture<?>[0]))
                        .thenCompose(ignored -> ElasticsearchStorage
                                .<BulkResponse>future(listener -> client.bulkAsync(request, OPTIONS, listener))
                                .thenApply(response -> {
                                    final SortedMap<BatchResponse.Key, Map<String, Object>> results = new TreeMap<>();
                                    final BulkItemResponse[] items = response.getItems();
                                    assert(items.length == responders.size());
                                    for(int i = 0; i != items.length; ++i) {
                                        results.putAll(responders.get(i).apply(items[i]));
                                    }
                                    return new BatchResponse.Basic(results);
                                }));
            }
        };
    }

    private Map<String, Object> toSource(final ObjectSchema schema, final Map<String, Object> data) {

        final Map<String, Object> source = new HashMap<>();
        final Mappings mappings = routing.mappings(schema);
        mappings.getProperties().forEach((name, type) -> {
            final Object value = data.get(name);
            source.put(name, type.toSource(value));
        });
        return source;
    }

    private Map<String, Object> fromSource(final ObjectSchema schema, final Map<String, Object> source) {

        final Map<String, Object> data = new HashMap<>();
        final Mappings mappings = routing.mappings(schema);
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

    private static <T> CompletableFuture<T> future(final ListenerConsumer<T> with) {

        final CompletableFuture<T> future = new CompletableFuture<>();
        try {
            with.accept(new ActionListener<T>() {
                @Override
                public void onResponse(final T t) {
                    future.complete(t);
                }

                @Override
                public void onFailure(final Exception e) {
                    future.completeExceptionally(e);
                }
            });
        } catch (final IOException e) {
            future.completeExceptionally(e);
        }
        return future;
    }

    private interface ListenerConsumer<T> {

        void accept(ActionListener<T> listener) throws IOException;
    }
}

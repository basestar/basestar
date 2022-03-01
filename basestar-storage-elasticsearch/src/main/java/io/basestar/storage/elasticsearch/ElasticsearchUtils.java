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

import com.google.common.collect.ImmutableMap;
import io.basestar.schema.LinkableSchema;
import io.basestar.storage.elasticsearch.mapping.Mappings;
import io.basestar.storage.elasticsearch.mapping.Settings;
import io.basestar.storage.util.CountPreservingTokenInfo;
import io.basestar.storage.util.KeysetPagingUtils;
import io.basestar.util.Name;
import io.basestar.util.Page;
import io.basestar.util.Sort;
import io.basestar.util.Throwables;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.HttpAsyncResponseConsumerFactory;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.PutMappingRequest;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

@Slf4j
public class ElasticsearchUtils {

    // FIXME: make configurable
    public static final RequestOptions OPTIONS;

    static {
        final RequestOptions.Builder builder = RequestOptions.DEFAULT.toBuilder();
        builder.setHttpAsyncResponseConsumerFactory(
                new HttpAsyncResponseConsumerFactory
                        .HeapBufferedResponseConsumerFactory(10000000));
        OPTIONS = builder.build();
    }

    public static Map<String, ?> createIndexSource(final Mappings mappings, final Settings settings) {

        return ImmutableMap.of(
                "settings", settings.all(),
                "mappings", mappings.source()
        );
    }

    public static CompletableFuture<AcknowledgedResponse> putMappings(final RestHighLevelClient client, final String name, final Mappings mappings) {

        return ElasticsearchUtils.future(listener -> client.indices().putMappingAsync(new PutMappingRequest(name)
                .source(mappings.source()), OPTIONS, listener));
    }

    public static CompletableFuture<AcknowledgedResponse> putDynamicSettings(final RestHighLevelClient client, final String name, final Settings settings) {

        return ElasticsearchUtils.future(listener -> client.indices().putSettingsAsync(new UpdateSettingsRequest(name)
                .settings(settings.dynamic()), OPTIONS, listener));
    }

    public static CompletableFuture<?> syncIndex(final RestHighLevelClient client, final String name, final Mappings mappings, final Settings settings) {

        return ElasticsearchUtils.<CreateIndexResponse>future(listener ->
                client.indices().createAsync(new CreateIndexRequest(name).source(createIndexSource(mappings, settings)), OPTIONS, listener))
                .exceptionally(e -> {
                    final Optional<ElasticsearchStatusException> statusException = Throwables.find(e, ElasticsearchStatusException.class);
                    if (statusException.map(s -> s.getDetailedMessage().contains("resource_already_exists_exception")).orElse(false)) {
                        return null;
                    } else {
                        throw Throwables.findRuntimeCause(e).orElseGet(() -> new CompletionException(e));
                    }
                }).thenCompose(v -> {
                    if (v == null) {
                        return CompletableFuture.allOf(
                                putMappings(client, name, mappings),
                                putDynamicSettings(client, name, settings)
                        );
                    } else {
                        return CompletableFuture.completedFuture(null);
                    }
                });
    }

    public static <T> CompletableFuture<T> future(final ListenerConsumer<T> with) {

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

    public static SortBuilder<?> sort(final Sort sort) {

        return SortBuilders.fieldSort(sort.getName().toString())
                .order(order(sort.getOrder()))
                .missing(sort.getNulls() == Sort.Nulls.FIRST ? "_first" : "_last");
    }

    public static SortOrder order(final Sort.Order order) {

        return order == Sort.Order.ASC ? SortOrder.ASC : SortOrder.DESC;
    }

    public interface ListenerConsumer<T> {

        void accept(ActionListener<T> listener) throws IOException;
    }

    public static QueryBuilder pagingQueryBuilder(final LinkableSchema schema, final List<Sort> sort, final Page.Token token) {

        final CountPreservingTokenInfo countPreservingTokenInfo = KeysetPagingUtils.countPreservingTokenInfo(token);
        final List<Object> sorting = KeysetPagingUtils.keysetValues(schema, sort, countPreservingTokenInfo.getToken());
        final BoolQueryBuilder outer = QueryBuilders.boolQuery();
        for (int i = 0; i < sort.size(); ++i) {
            if (i == 0) {
                outer.should(pagingRange(sort.get(i), sorting.get(i)));
            } else {
                final BoolQueryBuilder inner = QueryBuilders.boolQuery();
                for (int j = 0; j < i; ++j) {
                    final Name name = sort.get(j).getName();
                    final Object value = sorting.get(j);
                    if (value == null) {
                        inner.mustNot(QueryBuilders.existsQuery(name.toString()));
                    } else {
                        inner.must(QueryBuilders.termQuery(name.toString(), sorting.get(j)));
                    }
                }
                inner.must(pagingRange(sort.get(i), sorting.get(i)));
                outer.should(inner);
            }
        }
        return outer;
    }

    public static QueryBuilder pagingRange(final Sort sort, final Object value) {

        final String name = sort.getName().toString();
        if (sort.getOrder() == Sort.Order.ASC) {
            return QueryBuilders.rangeQuery(name).gt(value);
        } else {
            return QueryBuilders.rangeQuery(name).lt(value);
        }
    }
}

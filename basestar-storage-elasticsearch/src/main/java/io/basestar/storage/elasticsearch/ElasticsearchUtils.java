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
import io.basestar.storage.elasticsearch.mapping.Mappings;
import io.basestar.storage.elasticsearch.mapping.Settings;
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

import java.io.IOException;
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
                    if(statusException.map(s -> s.getDetailedMessage().contains("resource_already_exists_exception")).orElse(false)) {
                        return null;
                    } else {
                        throw Throwables.findRuntimeCause(e).orElseGet(() -> new CompletionException(e));
                    }
                }).thenCompose(v -> {
                    if(v == null) {
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

    public interface ListenerConsumer<T> {

        void accept(ActionListener<T> listener) throws IOException;
    }
}

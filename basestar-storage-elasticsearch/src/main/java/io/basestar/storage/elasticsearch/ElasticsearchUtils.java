package io.basestar.storage.elasticsearch;

import com.google.common.collect.ImmutableMap;
import io.basestar.storage.elasticsearch.mapping.Mappings;
import io.basestar.storage.elasticsearch.mapping.Settings;
import io.basestar.util.CompletableFutures;
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
import java.util.concurrent.CompletableFuture;

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
                client.indices().createAsync(new CreateIndexRequest(name)
                        .source(createIndexSource(mappings, settings)), OPTIONS, listener))
                .handle((result, error) -> {
                    if(error != null) {
                        //FIXME:
                        if(error instanceof ElasticsearchStatusException && ((ElasticsearchStatusException) error).getDetailedMessage().contains("resource_already_exists_exception")) {
                            return CompletableFuture.allOf(
                                    putMappings(client, name, mappings),
                                    putDynamicSettings(client, name, settings)
                            );
                        } else {
                            return CompletableFutures.completedExceptionally(error);
                        }
                    } else {
                        return result;
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

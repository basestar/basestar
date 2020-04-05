package io.basestar.storage.elasticsearch;

import com.google.common.collect.Multimap;
import io.basestar.schema.Namespace;
import io.basestar.storage.Storage;
import io.basestar.storage.TestStorage;
import io.basestar.storage.elasticsearch.mapping.Mappings;
import io.basestar.storage.elasticsearch.mapping.Settings;
import io.basestar.test.ContainerSpec;
import io.basestar.test.TestContainers;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.jupiter.api.BeforeAll;

import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;


public class TestElasticsearchStorage extends TestStorage {

    private static final int PORT = 9200;

    @BeforeAll
    public static void startLocalStack() {

        TestContainers.ensure(ContainerSpec.builder()
                .image("docker.elastic.co/elasticsearch/elasticsearch:7.6.1")
                .env("discovery.type=single-node")
                .port(PORT)
                .waitFor(Pattern.compile(".*Active license is now.*"))
                .build()).join();
    }

    @Override
    protected Storage storage(final Namespace namespace, final Multimap<String, Map<String, Object>> data) {

        final ElasticsearchRouting routing = ElasticsearchRouting.Simple.builder()
                .objectPrefix(UUID.randomUUID().toString() + "-")
                .historyPrefix(UUID.randomUUID().toString() + "-")
                .settings(Settings.builder()
                        .shards(1)
                        .replicas(0)
                        .build())
                .mappingsFactory(new Mappings.Factory.Default())
                .build();

        final RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", PORT, "http")));

        final Storage storage = ElasticsearchStorage.builder()
                .setClient(client)
                .setRouting(routing)
                .build();

        writeAll(storage, namespace, data);

        return storage;
    }

    @Override
    public void testLarge() {

        // Skipped
    }
}

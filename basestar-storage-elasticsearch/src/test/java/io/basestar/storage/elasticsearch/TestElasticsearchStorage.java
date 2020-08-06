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

import java.util.UUID;
import java.util.regex.Pattern;


public class TestElasticsearchStorage extends TestStorage {

    private static final int PORT = 9200;

    @BeforeAll
    public static void startLocalStack() {

        TestContainers.ensure(ContainerSpec.builder()
                .image("docker.elastic.co/elasticsearch/elasticsearch:7.4.0")
                .env("discovery.type=single-node")
                .port(PORT)
                .waitFor(Pattern.compile(".*Active license is now.*"))
                .build()).join();
    }

    @Override
    protected Storage storage(final Namespace namespace) {

        final ElasticsearchStrategy strategy = ElasticsearchStrategy.Simple.builder()
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

        return ElasticsearchStorage.builder()
                .setClient(client)
                .setStrategy(strategy)
                .build();
    }

    @Override
    public boolean supportsLike() {

        return true;
    }

    @Override
    public void testMultiValueIndex() {

        // Skipped
    }

    @Override
    public void testLarge() {

        // Skipped
    }
}

package io.basestar.spark.elasticsearch;

/*-
 * #%L
 * basestar-spark-elasticsearch
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
import io.basestar.schema.Namespace;
import io.basestar.schema.ObjectSchema;
import io.basestar.schema.Property;
import io.basestar.schema.use.UseString;
import io.basestar.spark.elasticsearch.sink.ElasticsearchSink;
import io.basestar.spark.transform.InstanceTransform;
import io.basestar.storage.elasticsearch.ElasticsearchStrategy;
import io.basestar.storage.elasticsearch.mapping.Mappings;
import io.basestar.storage.elasticsearch.mapping.Settings;
import io.basestar.test.ContainerSpec;
import io.basestar.test.TestContainers;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.UUID;
import java.util.regex.Pattern;

public class TestElasticsearchSink {

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

    @Test
    public void testImport() {

        final SparkSession session = SparkSession.builder()
                .master("local[*]")
                .getOrCreate();

        final Namespace namespace = Namespace.builder()
                .setSchema("Test1", ObjectSchema.builder()
                        .setProperty("hello", Property.builder()
                                .setType(UseString.DEFAULT)))
                .build();

        final ObjectSchema schema = namespace.requireObjectSchema("Test1");

        final ElasticsearchStrategy strategy = ElasticsearchStrategy.Simple.builder()
                .objectPrefix(UUID.randomUUID().toString() + "-")
                .mappingsFactory(new Mappings.Factory.Default())
                .settings(Settings.builder().build())
                .build();

        final ElasticsearchSink sink = ElasticsearchSink.builder()
                .hostName("localhost")
                .port(PORT)
                .protocol("http")
                .indexName(strategy.objectIndex(schema))
                .mappings(strategy.mappings(schema))
                .settings(strategy.settings(schema))
                .build();

        final Dataset<Row> ds = session.createDataFrame(ImmutableList.of(
                new Test1("hello"),
                new Test1("world")
        ), Test1.class);

        final InstanceTransform transform = InstanceTransform.builder().schema(schema).build();

        transform.then(sink).accept(ds);
    }

    @Data
    @NoArgsConstructor
    public static class Test1 {

        private String id;

        private String hello;

        private String schema;

        private String created;

        private String updated;

        private String hash;

        private Long version;

        public Test1(final String hello) {

            this.id = UUID.randomUUID().toString();
            this.hello = hello;
            this.schema = "Test1";
            this.version = 1L;
        }
    }
}

//package io.basestar.spark.elasticsearch.sink;
//
///*-
// * #%L
// * basestar-spark-elasticsearch
// * %%
// * Copyright (C) 2019 - 2020 Basestar.IO
// * %%
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// *      http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// * #L%
// */
//
//import io.basestar.schema.Reserved;
//import io.basestar.spark.sink.Sink;
//import io.basestar.spark.util.ScalaUtils;
//import io.basestar.storage.elasticsearch.ElasticsearchUtils;
//import io.basestar.storage.elasticsearch.mapping.Mappings;
//import io.basestar.storage.elasticsearch.mapping.Settings;
//import io.basestar.util.Nullsafe;
//import lombok.extern.slf4j.Slf4j;
//import org.apache.http.HttpHost;
//import org.apache.spark.rdd.RDD;
//import org.elasticsearch.client.RestClient;
//import org.elasticsearch.client.RestHighLevelClient;
//import org.elasticsearch.spark.rdd.EsSpark;
//
//import javax.annotation.Nonnull;
//import javax.annotation.Nullable;
//import java.util.HashMap;
//import java.util.Map;
//
//@Slf4j
//public class ElasticsearchSink implements Sink<RDD<Map<String, Object>>> {
//
//    private static final String DEFAULT_BATCH_BYTES = "1mb";
//
//    private static final int DEFAULT_BATCH_LIMIT = 1000;
//
//    @Nonnull
//    private final String hostName;
//
//    @Nonnull
//    private final int port;
//
//    @Nonnull
//    private final String protocol;
//
//    @Nonnull
//    private final String indexName;
//
//    @Nonnull
//    private final String typeName;
//
//    @Nonnull
//    private final Mappings mappings;
//
//    @Nonnull
//    private final Settings settings;
//
//    @Nonnull
//    private final String id;
//
//    @Nullable
//    private final String pipeline;
//
//    @Nonnull
//    private final String batchBytes;
//
//    private final int batchEntries;
//
//    @lombok.Builder(builderClassName = "Builder")
//    ElasticsearchSink(final String hostName, final Integer port, final String protocol, final String indexName,
//                      final String typeName, final Mappings mappings, final Settings settings, final String id,
//                      final String pipeline, final String batchBytes, final Integer batchEntries) {
//
//        this.hostName = Nullsafe.require(hostName);
//        this.port = Nullsafe.require(port);
//        this.protocol = Nullsafe.option(protocol, "http");
//        this.indexName = Nullsafe.require(indexName);
//        this.typeName = Nullsafe.option(typeName, "_doc");
//        this.mappings = Nullsafe.require(mappings);
//        this.settings = Nullsafe.require(settings);
//        this.id = Nullsafe.option(id, Reserved.ID);
//        this.pipeline = pipeline;
//        this.batchBytes = Nullsafe.option(batchBytes, DEFAULT_BATCH_BYTES);
//        this.batchEntries = Nullsafe.option(batchEntries, DEFAULT_BATCH_LIMIT);
//    }
//
//    @Override
//    public void accept(final RDD<Map<String, Object>> input) {
//
//        initializeIndex();
//        try {
//
//            final boolean ssl = "https".equalsIgnoreCase(protocol);
//
//            final Map<String, String> cfg = new HashMap<>();
//            cfg.put("es.nodes", hostName);
//            cfg.put("es.port", Integer.toString(port));
//            cfg.put("es.net.ssl", ssl ? "true" : "false");
//            cfg.put("es.nodes.wan.only", "true");
//            cfg.put("es.mapping.id", id);
//            cfg.put("es.batch.size.bytes", batchBytes);
//            cfg.put("es.batch.size.entries", Integer.toString(batchEntries));
//            cfg.put("es.resource", indexName + "/" + typeName);
//            if (pipeline != null) {
//                cfg.put("es.ingest.pipeline", pipeline);
//            }
//            log.info("Importing using configuration {}", cfg);
//            EsSpark.saveToEs(input, ScalaUtils.asScalaMap(cfg));
//
//        } finally {
//            restoreSettings();
//        }
//    }
//
//    private RestHighLevelClient client() {
//
//        return new RestHighLevelClient(
//                RestClient.builder(
//                        new HttpHost(hostName, port, protocol)));
//    }
//
//    private void initializeIndex() {
//
//        try (final RestHighLevelClient client = client()) {
//            final Settings fastSettings = settings.toBuilder().refreshInterval("-1").build();
//            ElasticsearchUtils.syncIndex(client, indexName, mappings, fastSettings).join();
//        } catch (final Exception e) {
//            log.error("Failed to initialize index", e);
//            throw new IllegalStateException(e);
//        }
//    }
//
//    private void restoreSettings() {
//
//        try (final RestHighLevelClient client = client()) {
//            ElasticsearchUtils.putDynamicSettings(client, indexName, settings).join();
//        } catch (final Exception e) {
//            log.error("Failed to restore settings", e);
//            throw new IllegalStateException(e);
//        }
//    }
//}

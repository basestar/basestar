package io.basestar.spark;

/*-
 * #%L
 * basestar-spark
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

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.catalog.CatalogTablePartition;
import org.apache.spark.sql.catalyst.catalog.ExternalCatalog;
import scala.Option;
import scala.collection.Seq;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * Extract partition information from an external table, and create a reader for each partition.
 *
 * @see PartitionedUpsertSink
 */

public class PartitionedUpsertSource extends PartitionedUpsert implements Source<Map<Map<String, String>, Dataset<Row>>> {

    private final SparkSession session;

    private final String databaseName;

    private final String tableName;

    private final Format format;

    public PartitionedUpsertSource(final SparkSession session, final String databaseName, final String tableName, final Format format) {

        this.session = session;
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.format = format;
    }

    public PartitionedUpsertSource(final SparkSession session, final String databaseName, final String tableName) {

        this(session, databaseName, tableName, Format.PARQUET);
    }

    @Override
    public void then(final Sink<Map<Map<String, String>, Dataset<Row>>> sink) {

        final ExternalCatalog catalog = session.sharedState().externalCatalog();

        final Map<Map<String, String>, Dataset<Row>> partitions = new HashMap<>();
        final Seq<CatalogTablePartition> parts = catalog.listPartitions(databaseName, tableName, Option.empty());
        final Map<Map<String, String>, String> latestPartition = new HashMap<>();
        final Map<Map<String, String>, CatalogTablePartition> latest = new HashMap<>();
        parts.foreach(ScalaUtils.scalaFunction(part -> {
            final Map<String, String> spec = new HashMap<>(ScalaUtils.asJavaMap(part.spec()));
            final String upsert = spec.get(UPSERT_PARTITION);
            if(upsert != null) {
                spec.remove(upsert);
                if(upsert.compareTo(latestPartition.getOrDefault(spec, "")) > 0) {
                    latestPartition.put(spec, upsert);
                } else {
                    return null;
                }
            }
            latest.put(spec, part);
            return null;
        }));

        latest.forEach((spec, part) -> {

            final URI location = part.location();
            final Dataset<Row> dataset = session.read().format(format.getSparkFormat()).load(location.toString());
            partitions.put(spec, dataset);
        });
        sink.accept(partitions);
    }
}

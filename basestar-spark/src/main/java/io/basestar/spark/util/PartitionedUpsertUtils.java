package io.basestar.spark.util;

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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.basestar.schema.Reserved;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.catalog.*;
import org.apache.spark.sql.types.StructType;
import scala.Option;

import java.io.IOException;
import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class PartitionedUpsertUtils {

    public static final String UPSERT_PARTITION = Reserved.PREFIX + "upsert";

    protected static final String DELETE_TABLE_SQL = "DROP TABLE IF EXISTS %s.%s";

    protected static final String CREATE_TABLE_SQL = "CREATE EXTERNAL TABLE IF NOT EXISTS %s.%s (%s) PARTITIONED BY(%s STRING) STORED AS PARQUET LOCATION \"%s\" TBLPROPERTIES (\"parquet.compress\"=\"SNAPPY\")";

    protected static final String REPAIR_TABLE_SQL = "MSCK REPAIR TABLE %s.%s";

    public static String defaultUpsertId() {

        return Instant.now().toString().replaceAll("[:.Z\\-]", "") + "-" + UUID.randomUUID().toString();
    }

    public static void repairTable(final SparkSession session, final String databaseName, final String tableName,
                                   final StructType structType, final List<String> partitionColumns, final Format format,
                                   final URI location, final Map<String, String> properties) {

        final ExternalCatalog catalog = session.sharedState().externalCatalog();

        final CatalogStorageFormat storage = SparkUtils.storageFormat(format, location);

        final long now = Instant.now().toEpochMilli();

        final CatalogTable table = CatalogTable.apply(
                TableIdentifier.apply(tableName, Option.apply(databaseName)),
                CatalogTableType.EXTERNAL(),
                storage,
                structType,
                Option.apply("hive"),
                ScalaUtils.asScalaSeq(partitionColumns),
                Option.empty(),
                "basestar",
                now, now,
                "2.4.0",
                ScalaUtils.asScalaMap(properties),
                Option.empty(),
                Option.empty(),
                Option.empty(),
                ScalaUtils.emptyScalaSeq(),
                true, true,
                ScalaUtils.emptyScalaMap()
        );

        if(catalog.tableExists(databaseName, tableName)) {

            catalog.alterTable(table);

        } else {

            catalog.createTable(table, true);
        }

        repairPartitions(session, databaseName, tableName, location);
    }

    public static void repairPartitions(final SparkSession session, final String databaseName, final String tableName, final URI tableLocation) {

        final ExternalCatalog catalog = session.sharedState().externalCatalog();
        final CatalogTable table = catalog.getTable(databaseName, tableName);

        final List<String> names = ScalaUtils.asJavaList(table.partitionColumnNames());
        try {
            final Path path = new Path(tableLocation);
            final FileSystem fs = path.getFileSystem(session.sparkContext().hadoopConfiguration());
            final List<CatalogTablePartition> partitions = collectPartitions(fs, path, names, ImmutableMap.of());
            SparkUtils.syncTablePartitions(catalog, databaseName, tableName, partitions);
        } catch (final IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private static List<CatalogTablePartition> collectPartitions(final FileSystem fs, final Path path, final List<String> names, final Map<String, String> values) throws IOException {

        if(names.isEmpty()) {
            final URI location = lastUpsert(fs, path);
            final CatalogTablePartition partition = SparkUtils.partition(values, Format.PARQUET, location);
            return ImmutableList.of(partition);
        } else {
            final String name = names.get(0);

            final List<CatalogTablePartition> partitions = new ArrayList<>();
            for (final FileStatus status : fs.listStatus(path)) {
                if(status.isDirectory()) {
                    final Path newPath = status.getPath();
                    final String pathName = newPath.getName();
                    assert pathName.startsWith(name + "=");
                    final String value = pathName.substring(name.length() + 1);
                    final Map<String, String> newValues = ImmutableMap.<String, String>builder()
                            .putAll(values).put(name, value).build();
                    partitions.addAll(collectPartitions(fs, newPath, names.subList(1, names.size()), newValues));
                }
            }
            return partitions;
        }
    }

    public static URI lastUpsert(final FileSystem fs, final Path path) throws IOException {

        URI latest = null;
        for (final FileStatus status : fs.listStatus(path)) {
            if(status.isDirectory()) {
                final URI uri = status.getPath().toUri();
                if (latest == null || uri.toString().compareTo(latest.toString()) > 0) {
                    latest = uri;
                }
            }
        }
        return latest;
    }

    public static String extractUpsertId(final URI partitionLocation) {

        final String str = partitionLocation.toString();
        final Pattern pattern = Pattern.compile("^.*" + Pattern.quote(UPSERT_PARTITION) + "=([^/]+)/?$");
        final Matcher matcher = pattern.matcher(str);
        if(matcher.matches()) {
            return matcher.group(1);
        } else {
            throw new IllegalStateException();
        }
    }
}

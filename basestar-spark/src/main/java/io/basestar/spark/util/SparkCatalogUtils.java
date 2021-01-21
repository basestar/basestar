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
import io.basestar.util.Immutable;
import io.basestar.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.catalog.*;
import org.apache.spark.sql.types.StructType;
import scala.Option;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

public class SparkCatalogUtils {

    private static final String NO_PARTITION = "__HIVE_DEFAULT_PARTITION__";

    public static CatalogStorageFormat storageFormat(final Format format, final URI location) {

        return CatalogStorageFormat.apply(
                Option.apply(location),
                Option.apply(format.getHadoopInputFormat()),
                Option.apply(format.getHadoopOutputFormat()),
                Option.apply(format.getHadoopSerde()),
                true,
                ScalaUtils.emptyScalaMap()
        );
    }

    public static CatalogTablePartition partition(final Map<String, String> spec, final Format format, final URI location) {

        final long now = Instant.now().toEpochMilli();
        return CatalogTablePartition.apply(ScalaUtils.asScalaMap(spec), storageFormat(format, location), ScalaUtils.emptyScalaMap(), now, now, Option.empty());
    }

    public enum MissingPartitions {

        SKIP,
        DROP_AND_RETAIN,
        DROP_AND_PURGE
    }

    public static void syncTablePartitions(final ExternalCatalog catalog, final String databaseName, final String tableName, final List<CatalogTablePartition> partitions, final MissingPartitions missing) {

        final Map<scala.collection.immutable.Map<String, String>, CatalogTablePartition> target = ScalaUtils.asJavaStream(catalog.listPartitions(databaseName, tableName, Option.empty()))
                .collect(Collectors.toMap(CatalogTablePartition::spec, v -> v));

        final Map<scala.collection.immutable.Map<String, String>, CatalogTablePartition> source = partitions.stream()
                .collect(Collectors.toMap(CatalogTablePartition::spec, v -> v));

        final List<CatalogTablePartition> create = new ArrayList<>();
        final List<CatalogTablePartition> alter = new ArrayList<>();
        final Set<scala.collection.immutable.Map<String, String>> drop = new HashSet<>();

        final Set<scala.collection.immutable.Map<String, String>> union = new HashSet<>();
        union.addAll(target.keySet());
        union.addAll(source.keySet());
        for(final scala.collection.immutable.Map<String, String> spec : union) {
            final CatalogTablePartition before = target.get(spec);
            final CatalogTablePartition after = source.get(spec);
            if(after == null) {
                drop.add(spec);
            } else if(before == null) {
                create.add(after);
            } else {
                alter.add(after);
            }
        }

        if(!create.isEmpty()) {
            catalog.createPartitions(databaseName, tableName, ScalaUtils.asScalaSeq(create), false);
        }
        if(!alter.isEmpty()) {
            catalog.alterPartitions(databaseName, tableName, ScalaUtils.asScalaSeq(alter));
        }
        if(missing != MissingPartitions.SKIP && !drop.isEmpty()) {
            final boolean purge = missing != MissingPartitions.DROP_AND_PURGE;
            catalog.dropPartitions(databaseName, tableName, ScalaUtils.asScalaSeq(drop), false, purge, !purge);
        }
    }

    public static CatalogDatabase ensureDatabase(final ExternalCatalog catalog, final String databaseName, final String location) {

        if(!catalog.databaseExists(databaseName)) {
            catalog.createDatabase(CatalogDatabase.apply(databaseName, databaseName, URI.create(location), ScalaUtils.emptyScalaMap()), true);
        }
        return catalog.getDatabase(databaseName);
    }

    public static boolean tableExists(final ExternalCatalog catalog, final String databaseName, final String tableName) {

        if(!catalog.databaseExists(databaseName)) {
            return false;
        }
        return catalog.tableExists(databaseName, tableName);
    }

    public static CatalogTable ensureTable(final ExternalCatalog catalog, final String databaseName, final String tableName,
                                           final StructType structType, final List<String> partitionColumns, final Format format,
                                           final URI location, final Map<String, String> properties) {

        final CatalogStorageFormat storage = SparkCatalogUtils.storageFormat(format, location);

        final long now = Instant.now().toEpochMilli();

        final long created;
        final boolean exists = catalog.tableExists(databaseName, tableName);
        final Option<CatalogStatistics> stats;
        if(exists) {
            final CatalogTable table = catalog.getTable(databaseName, tableName);
            created = table.createTime();
            stats = table.stats();
        } else {
            created = now;
            stats = Option.empty();
        }

        final CatalogTable table = CatalogTable.apply(
                TableIdentifier.apply(tableName, Option.apply(databaseName)),
                CatalogTableType.EXTERNAL(),
                storage,
                structType,
                Option.apply("hive"),
                ScalaUtils.asScalaSeq(partitionColumns),
                Option.empty(),
                "basestar",
                created, now,
                "2.4.0",
                ScalaUtils.asScalaMap(properties),
                stats,
                Option.empty(),
                Option.empty(),
                ScalaUtils.emptyScalaSeq(),
                true, true,
                ScalaUtils.emptyScalaMap(),
                Option.empty()
        );

        if(exists) {

            catalog.alterTable(table);

        } else {

            catalog.createTable(table, true);
        }

        return catalog.getTable(databaseName, tableName);
    }

//    public static void repairPartitions(final ExternalCatalog catalog, final Configuration configuration, final String databaseName, final String tableName, final URI tableLocation) {
//
//        final List<CatalogTablePartition> partitions = collectPartitions(catalog, configuration, databaseName, tableName, tableLocation);
//        if(!partitions.isEmpty()) {
//            syncTablePartitions(catalog, databaseName, tableName, partitions, true);
//        }
//    }

    public static List<CatalogTablePartition> findPartitions(final ExternalCatalog catalog, final Configuration configuration,
                                                             final String databaseName, final String tableName, final URI tableLocation) {

        return findPartitions(catalog, configuration, databaseName, tableName, tableLocation, FindPartitionsStrategy.DEFAULT);
    }

    public static List<CatalogTablePartition> findPartitions(final ExternalCatalog catalog, final Configuration configuration,
                                                             final String databaseName, final String tableName, final URI tableLocation,
                                                             final FindPartitionsStrategy strategy) {

        final CatalogTable table = catalog.getTable(databaseName, tableName);
        final List<String> names = ScalaUtils.asJavaList(table.partitionColumnNames());
        try {
            final Path path = new Path(tableLocation);
            final FileSystem fs = path.getFileSystem(configuration);
            return findPartitions(fs, path, names, ImmutableMap.of(), strategy);
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static List<CatalogTablePartition> findPartitions(final FileSystem fs, final Path path,
                                                             final List<String> names, final Map<String, String> spec,
                                                             final FindPartitionsStrategy strategy) throws IOException {

        if(names.isEmpty()) {
            return strategy.location(fs, path).map(location -> {
                final CatalogTablePartition partition = SparkCatalogUtils.partition(spec, Format.PARQUET, location);
                return ImmutableList.of(partition);
            }).orElse(ImmutableList.of());
        } else {

            final String name = names.get(0);
            final List<CatalogTablePartition> partitions = new ArrayList<>();
            try {
                for (final FileStatus status : fs.listStatus(path)) {
                    if (status.isDirectory()) {
                        final Path newPath = status.getPath();
                        final String pathName = newPath.getName();
                        final Pair<String, String> entry = fromPartitionPathTerm(pathName);
                        final String key = entry.getFirst();
                        assert key.equals(name);
                        final String value = entry.getSecond();
                        if(strategy.include(spec, key, value)) {
                            final Map<String, String> newValues = Immutable.put(spec, name, value);
                            partitions.addAll(findPartitions(fs, newPath, names.subList(1, names.size()), newValues, strategy));
                        }
                    }
                }
            } catch (final FileNotFoundException e) {
                // suppress
            }
            return partitions;
        }
    }

    public interface FindPartitionsStrategy {

        Default DEFAULT = new Default();

        boolean include(Map<String, String> spec, String key, String value);

        Optional<URI> location(FileSystem fileSystem, Path path);

        class Default implements FindPartitionsStrategy {

            @Override
            public boolean include(final Map<String, String> spec, final String key, final String value) {

                return true;
            }

            @Override
            public Optional<URI> location(final FileSystem fileSystem, final Path path) {

                return Optional.of(path.toUri());
            }
        }
    }

    public static String toPartitionPathTerm(final String key, final String value) {

        assert key != null && value != null;
        return key + "=" + (value.isEmpty() ? NO_PARTITION : value);
    }

    public static Pair<String, String> fromPartitionPathTerm(final String path) {

        final int index = path.indexOf("=");
        assert index >= 0;
        final String key = path.substring(0, index);
        final String value = path.substring(index + 1);
        return Pair.of(key, value.equals(NO_PARTITION) ? "" : value);
    }

    public static URI partitionLocation(final URI location, final List<Pair<String, String>> spec) {

        return URI.create(location.toString() + "/" + spec.stream().map(v -> toPartitionPathTerm(v.getFirst(), v.getSecond()))
                .collect(Collectors.joining("/")));
    }

    public static String escapeName(final String ... names) {

        return Arrays.stream(names).map(SparkCatalogUtils::escapeName)
                .collect(Collectors.joining("."));
    }

    private static String escapeName(final String name) {

        // Escapes common characters in names like hyphen, dot, etc,
        // not sure how to escape backticks but not likely to appear anyway
        if(name.contains("`")) {
            throw new IllegalStateException("Invalid name");
        }
        return "`" + name + "`";
    }
}

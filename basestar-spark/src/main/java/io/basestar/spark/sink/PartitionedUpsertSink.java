package io.basestar.spark.sink;

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
import io.basestar.schema.ObjectSchema;
import io.basestar.schema.Reserved;
import io.basestar.spark.util.*;
import io.basestar.util.Nullsafe;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.catalog.CatalogTablePartition;
import org.apache.spark.sql.catalyst.catalog.ExternalCatalog;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Option;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class PartitionedUpsertSink extends PartitionedUpsertUtils implements Sink<Dataset<Row>> {

    private static final Integer DEFAULT_MERGE_COUNT = 100;

    private static final String STATE_COLUMN = Reserved.PREFIX + "state";

    private static final String CREATE_STATE = "CREATE";

    private static final String UPDATE_STATE = "UPDATE";

    private static final String DELETE_STATE = "DELETE";

    private static final String IGNORE_STATE = "IGNORE";

    private final String databaseName;

    private final String tableName;

    private final List<String> idColumns;

    private final String upsertId;

    private final Format format;

    private final String deletedColumn;

    private final int mergeCount;

    @lombok.Builder(builderClassName = "Builder")
    PartitionedUpsertSink(final String databaseName, final String tableName, final List<String> idColumns,
                          final String upsertId, final Format format, final String deletedColumn, final Integer mergeCount) {

        this.databaseName = databaseName;
        this.tableName = tableName;
        this.idColumns = Nullsafe.orDefault(idColumns, ImmutableList.of(ObjectSchema.ID));
        this.upsertId = Nullsafe.orDefault(upsertId, PartitionedUpsertSink::defaultUpsertId);
        this.format = Nullsafe.orDefault(format, Format.PARQUET);
        this.deletedColumn = Nullsafe.orDefault(deletedColumn, Reserved.DELETED);
        this.mergeCount = Nullsafe.orDefault(mergeCount, DEFAULT_MERGE_COUNT);
    }

    private Dataset<Row> clean(final Dataset<Row> input) {

        Dataset<Row> output = input;
        final List<String> fieldNames = Arrays.asList(input.schema().fieldNames());
        // Remove upsert partition info in case this is some kind of chain
        if(!fieldNames.contains(UPSERT_PARTITION)) {
            output = output.drop(UPSERT_PARTITION);
        }
        if(fieldNames.contains(deletedColumn)) {
            output = output.withColumn(STATE_COLUMN, functions.when(output.col(deletedColumn), DELETE_STATE).otherwise(UPDATE_STATE)).drop(deletedColumn);
        } else {
            output = output.withColumn(STATE_COLUMN, functions.lit(UPDATE_STATE));
        }
        return output;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Partition implements Serializable {

        private String[] values;

        public Map<String, String> spec(final List<String> names) {

            final Map<String, String> result = new HashMap<>();
            for(int i = 0; i != names.size(); ++i) {
                result.put(names.get(i), values[i]);
            }
            return result;
        }

        @Override
        public boolean equals(final Object other) {

            if(other instanceof Partition) {
                return Arrays.deepEquals(values, ((Partition) other).values);
            } else {
                return false;
            }
        }

        @Override
        public int hashCode() {

            return Arrays.deepHashCode(values);
        }

        @Override
        public String toString() {

            return String.join(",", values);
        }

        /**
         * Has trailing slash
         */

        public String buildPath(final List<String> names) {

            final StringBuilder result = new StringBuilder();
            for(int i = 0; i != names.size(); ++i) {
                result.append(names.get(i)).append("=").append(values[i]).append("/");
            }
            return result.toString();
        }
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class PartitionState implements Serializable {

        private Partition partition;

        private String state;

        @Override
        public String toString() {

            return partition + "=" + state;
        }
    }

    private static Partition partition(final Row row, final int[] partitionIndexes) {

        final String[] values = new String[partitionIndexes.length];
        for(int i = 0; i != partitionIndexes.length; ++i) {
            values[i] = row.getString(partitionIndexes[i]);
        }
        return new Partition(values);
    }

    private static Map<Partition, CatalogTablePartition> existing(final ExternalCatalog catalog, final String databaseName, final String tableName,
                                                                  final List<String> partitionColumns, final Dataset<Row> changes) {

        final StructType dataSchema = changes.schema();
        final int[] partitionIndexes = partitionColumns.stream().mapToInt(dataSchema::fieldIndex).toArray();

        final List<Partition> partitionValues = changes
                .map((MapFunction<Row, Partition>) row -> partition(row, partitionIndexes), Encoders.bean(Partition.class))
                .distinct().collectAsList();

        final Map<Partition, CatalogTablePartition> result = new HashMap<>();
        partitionValues.forEach(v -> {

            final Map<String, String> spec = v.spec(partitionColumns);
            final Option<CatalogTablePartition> partition = catalog.getPartitionOption(databaseName, tableName, ScalaUtils.asScalaMap(spec));
            if(partition.isDefined()) {
                result.put(v, partition.get());
            }
        });
        return result;
    }

    private static Map<Partition, Set<String>> states(final List<String> partitionColumns, final Dataset<Row> joined) {

        final StructType dataSchema = joined.schema();
        final int stateIndex = dataSchema.fieldIndex(STATE_COLUMN);
        final int[] partitionIndexes = partitionColumns.stream().mapToInt(dataSchema::fieldIndex).toArray();

        final List<PartitionState> states = joined.map((MapFunction<Row, PartitionState>) row -> {
            final Partition partition = partition(row, partitionIndexes);
            final String state = row.getString(stateIndex);
            return new PartitionState(partition, state);
        }, Encoders.bean(PartitionState.class)).distinct().collectAsList();

        return states.stream().collect(Collectors.groupingBy(
                PartitionState::getPartition,
                Collectors.mapping(PartitionState::getState, Collectors.toSet())
        ));
    }

    private static Dataset<Row> load(final SQLContext sqlContext, final URI basePath, final Format format,
                                     final StructType structType, final Collection<CatalogTablePartition> partitions) {

        if(partitions.isEmpty()) {
            return sqlContext.createDataFrame(ImmutableList.of(), structType);
        } else {
            return sqlContext.read().format(format.getSparkFormat())
                    .option("basePath", basePath.toString())
                    .schema(structType)
                    .load(partitions.stream().map(v -> v.location().toString()).toArray(String[]::new))
                    .withColumn(STATE_COLUMN, functions.lit(IGNORE_STATE))
                    .map((MapFunction<Row, Row>) row -> SparkSchemaUtils.conform(row, structType), RowEncoder.apply(structType));
        }
    }

    private static Dataset<Row> join(final List<String> idColumns, final Dataset<Row> current, final Dataset<Row> changes) {

        final Column condition = idColumns.stream().map(col -> changes.col(col).equalTo(current.col(col)))
                .reduce(Column::and).orElseThrow(IllegalStateException::new);

        final StructType dataSchema = changes.schema();

        return changes.joinWith(current, condition, "full_outer")
                .map((MapFunction<Tuple2<Row, Row>, Row>) t -> {

                    if(t._1() != null) {
                        if(t._2() != null) {
                            return t._1();
                        } else {
                            return SparkSchemaUtils.set(t._1(), ImmutableMap.of(STATE_COLUMN, CREATE_STATE));
                        }
                    } else {
                        return SparkSchemaUtils.conform(t._2(), dataSchema);
                    }

                }, RowEncoder.apply(changes.schema()));
    }

    private Dataset<Row> output(final List<String> partitionColumns, final Dataset<Row> input, final Map<Partition, String> upsertIds) {

        final StructType inputSchema = input.schema();
        final StructField[] inputFields = inputSchema.fields();
        final StructField[] outputFields = new StructField[inputFields.length + 1];
        for(int i = 0; i != inputFields.length; ++i) {
            outputFields[i] = inputFields[i];
        }
        outputFields[inputFields.length] = SparkSchemaUtils.field(UPSERT_PARTITION, DataTypes.StringType);
        final StructType outputSchema = DataTypes.createStructType(outputFields);

        final int[] partitionIndexes = partitionColumns.stream().mapToInt(inputSchema::fieldIndex).toArray();
        return input.map((MapFunction<Row, Row>) row -> {

            final Partition partition = partition(row, partitionIndexes);

            final Object[] outputValues = new Object[outputFields.length];
            for(int i = 0; i != inputFields.length; ++i) {
                outputValues[i] = row.get(i);
            }
            outputValues[inputFields.length] = upsertIds.get(partition);
            return new GenericRow(outputValues);

        }, RowEncoder.apply(outputSchema));
    }

    private static String joinPaths(final String a, final String b) {

        return a.endsWith("/") ? a + b : a + "/" + b;
    }

    @Override
    public void accept(final Dataset<Row> input) {

        final SparkSession session = input.sparkSession();
        final SparkContext context = session.sparkContext();
        final SQLContext sqlContext = input.sqlContext();

        context.setJobDescription("Upsert to " + tableName);

        final ExternalCatalog catalog = session.sharedState().externalCatalog();
        final CatalogTable table = catalog.getTable(databaseName, tableName);
        final List<String> partitionColumns = ScalaUtils.asJavaList(table.partitionColumnNames());

        final URI tableLocation = table.location();

        final Dataset<Row> changes = clean(input).cache();

        final Map<Partition, CatalogTablePartition> existingPartitions = existing(catalog, databaseName, tableName, partitionColumns, changes);

        log.info("Upsert on {} refers to existing partitions: {}", tableName, existingPartitions.keySet());

        final Dataset<Row> current = load(sqlContext, tableLocation, format, changes.schema(), existingPartitions.values());

        final Dataset<Row> joined = join(idColumns, current, changes).cache();

        final Map<Partition, Set<String>> states = states(partitionColumns, joined);

        final AtomicInteger create = new AtomicInteger(0);
        final AtomicInteger merge = new AtomicInteger(0);
        final AtomicInteger append = new AtomicInteger(0);
        final Configuration hadoopConfiguration = context.hadoopConfiguration();
        final Map<Partition, String> upsertIds = states.entrySet().stream().collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> {
                    final Set<String> state = entry.getValue();
                    final CatalogTablePartition existing = existingPartitions.get(entry.getKey());
                    // If only creates then append to existing partition
                    if(existing == null) {
                        create.incrementAndGet();
                        return upsertId;
                    } else if(state.contains(UPDATE_STATE) || state.contains(DELETE_STATE)) {
                        merge.incrementAndGet();
                        return upsertId;
                    } else {
                        try {
                            final Path path = new Path(existing.location());
                            final FileSystem fs = path.getFileSystem(hadoopConfiguration);
                            final int files = fs.listStatus(path).length;
                            if(files > mergeCount) {
                                log.error("Merging {} because it has {} files", entry.getKey(), files);
                                merge.incrementAndGet();
                                return upsertId;
                            }
                        } catch (final IOException e) {
                            log.error("Failed to check file count for {}", existing.location(), e);
                        }
                        append.incrementAndGet();
                        return extractUpsertId(existing.location());
                    }
                }
        ));

        log.info("Upsert on {} has target states: {}", tableName, states);

        context.setJobDescription("Upsert to " + tableName + " (create: " + create + ", merge: " + merge + ", append: " + append + ")");

        final Dataset<Row> upsert = output(partitionColumns, joined, upsertIds);

        // If new upsert, remove any items in delete state, otherwise this is an append, only keep items in create state

        final Dataset<Row> filtered = upsert
                .filter(functions.when(upsert.col(UPSERT_PARTITION).equalTo(upsertId), upsert.col(STATE_COLUMN).notEqual(DELETE_STATE))
                        .otherwise(upsert.col(STATE_COLUMN).equalTo(CREATE_STATE)))
                .drop(STATE_COLUMN);

        final String[] outputPartitionNames = Stream.concat(partitionColumns.stream(), Stream.of(UPSERT_PARTITION)).toArray(String[]::new);
        final Column[] outputPartitions = Arrays.stream(outputPartitionNames).map(filtered::col).toArray(Column[]::new);
        final Dataset<Row> output = filtered.repartition(1, outputPartitions);

        output.write().format(format.getSparkFormat())
                .mode(SaveMode.Append)
                .partitionBy(outputPartitionNames)
                .save(tableLocation.toString());

        upsertIds.forEach((partition, upsertId) -> {

            if(this.upsertId.equals(upsertId)) {
                final Map<String, String> spec = partition.spec(partitionColumns);
                final String partitionPath = partition.buildPath(partitionColumns) + UPSERT_PARTITION + "=" + upsertId + "/";
                final URI newPartitionLocation = URI.create(joinPaths(tableLocation.toString(), partitionPath));
                final CatalogTablePartition newPartition = SparkUtils.partition(spec, format, newPartitionLocation);
                final CatalogTablePartition existing = existingPartitions.get(partition);
                if(existing == null) {
                    log.info("Creating partition {} with location {}", partition, newPartitionLocation);
                    catalog.createPartitions(databaseName, tableName, Option.apply(newPartition).toList(), false);
                } else {
                    log.info("Updating partition {} with location {}", partition, newPartitionLocation);
                    catalog.alterPartitions(databaseName, tableName, Option.apply(newPartition).toList());
                }
            }
        });

        joined.unpersist(true);
        changes.unpersist(true);

        context.setJobDescription(null);
    }
}

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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.basestar.schema.Reserved;
import io.basestar.util.Nullsafe;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.catalog.CatalogTablePartition;
import org.apache.spark.sql.catalyst.catalog.ExternalCatalog;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.StructType;
import scala.Option;
import scala.Tuple2;

import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class PartitionedUpsertSink extends PartitionedUpsert implements Sink<Map<Map<String, String>, Dataset<Row>>> {

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

    @lombok.Builder(builderClassName = "Builder")
    PartitionedUpsertSink(final String databaseName, final String tableName, final List<String> idColumns,
                          final String upsertId, final Format format, final String deletedColumn) {

        this.databaseName = databaseName;
        this.tableName = tableName;
        this.idColumns = Nullsafe.option(idColumns, ImmutableList.of(Reserved.ID));
        this.upsertId = Nullsafe.option(upsertId, PartitionedUpsertSink::defaultUpsertId);
        this.format = Nullsafe.option(format, Format.PARQUET);
        this.deletedColumn = Nullsafe.option(deletedColumn, Reserved.DELETED);
    }

    private Dataset<Row> clean(final Dataset<Row> input) {

        Dataset<Row> output = input;
        final List<String> fieldNames = Arrays.asList(input.schema().fieldNames());
        // Remove upsert partition info in case this is some kind of chain
        if(fieldNames.contains(UPSERT_PARTITION)) {
            output = output.drop(UPSERT_PARTITION);
        }
        if(fieldNames.contains(deletedColumn)) {
            output = output.withColumn(STATE_COLUMN, functions.when(output.col(deletedColumn), DELETE_STATE).otherwise(UPDATE_STATE)).drop(deletedColumn);
        } else {
            output = output.withColumn(STATE_COLUMN, functions.lit(UPDATE_STATE));
        }
        return output;
    }

    private static Dataset<Row> prepare(final Dataset<Row> input, final List<String> partitionColumns, final Map<String, String> partitionValues, final String upsertId) {

        Dataset<Row> output = input;
        final List<String> fieldNames = Arrays.asList(input.schema().fieldNames());
        output = output.withColumn(UPSERT_PARTITION, functions.lit(upsertId));
        for(final Map.Entry<String, String> entry : partitionValues.entrySet()) {
            if(!fieldNames.contains(entry.getKey())) {
                output = output.withColumn(entry.getKey(), functions.lit(entry.getValue()));
            }
        }
        output = output.coalesce(1);

        output = output.repartition(Stream.concat(partitionColumns.stream(), Stream.of(UPSERT_PARTITION))
                .map(output::col).toArray(Column[]::new));

        return output;
    }

    @Override
    public void accept(final Map<Map<String, String>, Dataset<Row>> inputs) {

        inputs.forEach((rawPartitionValues, input) -> {

            final SparkSession session = input.sparkSession();
            final SQLContext sqlContext = input.sqlContext();

            final ExternalCatalog catalog = session.sharedState().externalCatalog();
            final CatalogTable table = catalog.getTable(databaseName, tableName);
            final List<String> partitionColumns = ScalaUtils.asJavaList(table.partitionColumnNames());

            final String[] partitionNames = Stream.concat(partitionColumns.stream(), Stream.of(UPSERT_PARTITION)).toArray(String[]::new);

            final URI tableLocation = table.location();

            // Remove upsert partition from spec if it was passed
            final Map<String, String> partitionValues = rawPartitionValues.entrySet().stream()
                    .filter(e -> !UPSERT_PARTITION.equals(e.getKey()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            // Upsert partition is specified on output path only
            final String partitionPath = partitionColumns.stream()
                    .map(k -> k + "=" + partitionValues.get(k)).collect(Collectors.joining("/"))
                    + "/" + UPSERT_PARTITION + "=" + upsertId + "/";

            final URI newPartitionLocation = URI.create(joinPaths(tableLocation.toString(), partitionPath));

            final Dataset<Row> changes = clean(input);
            final StructType structType = changes.schema();

            final scala.collection.immutable.Map<String, String> partitionSpec = ScalaUtils.asScalaMap(partitionValues);
            final Option<CatalogTablePartition> partition = catalog.getPartitionOption(databaseName, tableName, partitionSpec);

            final Dataset<Row> current;
            final boolean create;
            final String oldUpsertId;
            if(partition.isDefined()) {
                final URI partitionLocation = partition.get().location();
                if (partitionLocation.equals(newPartitionLocation)) {
                    log.info("Partition for {} seems to already have been written as {}", partitionValues, newPartitionLocation);
                    return;
                }
                final String serde = partition.get().storage().serde().get();
                current = sqlContext.read().format(Format.forHadoopSerde(serde).getSparkFormat()).load(partitionLocation.toString())
                        .withColumn(STATE_COLUMN, functions.lit(IGNORE_STATE));
                create = false;
                oldUpsertId = extractUpsertId(partitionLocation);
            } else {
                current = sqlContext.createDataFrame(ImmutableList.of(), structType);
                create = true;
                oldUpsertId = null;
            }

            log.info("Writing partition {} as {}", partitionValues, newPartitionLocation);

            final Column condition = idColumns.stream().map(col -> changes.col(col).equalTo(current.col(col)))
                    .reduce(Column::and).orElseThrow(IllegalStateException::new);

            final Dataset<Row> merged = changes.joinWith(current, condition, "full_outer")
                    .map((MapFunction<Tuple2<Row, Row>, Row>) t -> {

                        if(t._1() != null) {
                            if(t._2() != null) {
                                return t._1();
                            } else {
                                return SparkSchemaUtils.with(t._1(), ImmutableMap.of(STATE_COLUMN, CREATE_STATE));
                            }
                        } else {
                            return SparkSchemaUtils.conform(t._2(), structType);
                        }

                    }, RowEncoder.apply(changes.schema()));

            final Dataset<Row> cached = merged.cache();

            final Set<String> states = cached.select(cached.col(STATE_COLUMN)).distinct().collectAsList()
                    .stream().map(v -> (String)SparkSchemaUtils.get(v, STATE_COLUMN)).collect(Collectors.toSet());

            if(create || states.contains(UPDATE_STATE) || states.contains(DELETE_STATE)) {

                final Dataset<Row> filtered = cached.filter(cached.col(STATE_COLUMN).notEqual(DELETE_STATE)).drop(STATE_COLUMN);

                final Dataset<Row> upsert = prepare(filtered, partitionColumns, partitionValues, upsertId);

                upsert.write().format(format.getSparkFormat())
                        .mode(SaveMode.Append)
                        .partitionBy(partitionNames)
                        .save(tableLocation.toString());

                final CatalogTablePartition newPartition = SparkUtils.partition(ScalaUtils.asJavaMap(partitionSpec), format, newPartitionLocation);
                if(create) {
                    catalog.createPartitions(databaseName, tableName, Option.apply(newPartition).toList(), false);
                } else {
                    catalog.alterPartitions(databaseName, tableName, Option.apply(newPartition).toList());
                }

            } else if(states.contains(CREATE_STATE)) {

                final Dataset<Row> filtered = cached.filter(cached.col(STATE_COLUMN).equalTo(CREATE_STATE)).drop(STATE_COLUMN);

                final Dataset<Row> append = prepare(filtered, partitionColumns, partitionValues, oldUpsertId);

                append.write().format(format.getSparkFormat())
                        .mode(SaveMode.Append)
                        .partitionBy(partitionNames)
                        .save(tableLocation.toString());
            }
        });
    }

    private String joinPaths(final String a, final String b) {

        return a.endsWith("/") ? a + b : a + "/" + b;
    }
}

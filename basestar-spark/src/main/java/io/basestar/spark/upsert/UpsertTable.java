package io.basestar.spark.upsert;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.basestar.schema.Reserved;
import io.basestar.spark.query.Query;
import io.basestar.spark.sink.Sink;
import io.basestar.spark.source.Source;
import io.basestar.spark.util.Format;
import io.basestar.spark.util.ScalaUtils;
import io.basestar.spark.util.SparkCatalogUtils;
import io.basestar.spark.util.SparkRowUtils;
import io.basestar.util.Immutable;
import io.basestar.util.Nullsafe;
import io.basestar.util.Pair;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.catalog.CatalogTablePartition;
import org.apache.spark.sql.catalyst.catalog.ExternalCatalog;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Option;
import scala.Tuple2;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class UpsertTable {

    protected static final Map<String, String> TABLE_PROPERTIES = ImmutableMap.of("parquet.compress", "SNAPPY");

    protected static final String SEQUENCE = Reserved.PREFIX + "sequence";

    protected static final String OPERATION = Reserved.PREFIX + "operation";

    protected static final String BEFORE = Reserved.PREFIX + "before";

    protected static final String AFTER = Reserved.PREFIX + "after";

    protected static final String NO_SEQUENCE = "";

    private static final Format FORMAT = Format.PARQUET;

    protected final String database;

    protected final String name;

    protected final StructType baseType;

    protected final String idColumn;

    protected final DataType idType;

    protected final List<String> basePartition;

    private final StructType deltaType;

    protected final List<String> deltaPartition;

    protected final String location;

    @lombok.Builder(builderClassName = "Builder")
    protected UpsertTable(final String database, final String name, final StructType structType,
                          final String idColumn, final String location, final List<String> partition) {

        this.database = Nullsafe.require(database);
        this.name = Nullsafe.require(name);
        this.idColumn = Nullsafe.require(idColumn);
        this.idType = SparkRowUtils.requireField(structType, idColumn).dataType();
        this.basePartition = Immutable.copy(partition);
        this.baseType = Nullsafe.require(structType);
        this.deltaType = createDeltaType(basePartition, idColumn, idType, structType);
        this.deltaPartition = Immutable.copyAddAll(ImmutableList.of(SEQUENCE, OPERATION), basePartition);
        if(Nullsafe.require(location).endsWith("/")) {
            this.location = location;
        } else {
            this.location = location + "/";
        }
    }

    private static StructType createDeltaType(final List<String> partition, final String idColumn,
                                              final DataType idType, final StructType structType) {

        final List<StructField> fields = new ArrayList<>();
        fields.add(SparkRowUtils.field(SEQUENCE, DataTypes.StringType));
        fields.add(SparkRowUtils.field(OPERATION, DataTypes.StringType));
        partition.forEach(name -> fields.add(SparkRowUtils.field(name, SparkRowUtils.requireField(structType, name).dataType())));
        fields.add(SparkRowUtils.field(idColumn, idType));
        fields.add(SparkRowUtils.field(BEFORE, structType));
        fields.add(SparkRowUtils.field(AFTER, structType));
        return DataTypes.createStructType(fields);
    }

    protected String baseTableName() {

        return name + "_base";
    }

    protected URI baseLocation() {

        return URI.create(location + "base");
    }

    protected String deltaTableName() {

        return name + "_delta";
    }

    protected URI deltaLocation() {

        return URI.create(location + "delta");
    }

    public Dataset<Row> selectBase(final SparkSession session) {

        final String tableName = baseTableName();
        return session.sqlContext().read()
                .table(database + "." + tableName)
                .select(baseColumns());
    }

    public Dataset<Row> selectDelta(final SparkSession session) {

        final String tableName = deltaTableName();
        return session.sqlContext().read()
                .table(database + "." + tableName)
                .select(deltaColumns());
    }

    public Dataset<Row> selectDeltaAfter(final SparkSession session, final Instant timestamp) {

        return selectDeltaAfter(session, sequence(timestamp, ""));
    }

    public Dataset<Row> selectDeltaAfter(final SparkSession session, final String sequence) {

        return selectDelta(session).filter(functions.col(SEQUENCE).gt(sequence));
    }

    protected Dataset<Row> latestDeltas(final Dataset<Row> input) {

        final String idColumn = this.idColumn;
        final UpsertReducer reducer = new UpsertReducer(deltaType);
        return input.select(deltaColumns())
                .groupBy(functions.col(idColumn))
                .agg(reducer.apply(deltaColumns()).as("_2"))
                .select(functions.col("_2.*"))
                .select(deltaColumns());
    }

    public Dataset<Row> selectLatestDelta(final SparkSession session) {

        return latestDeltas(selectDelta(session));
    }

    public Dataset<Row> select(final SparkSession session) {

        return baseWithDeltas(selectBase(session), selectLatestDelta(session));
    }

    private Dataset<Row> baseWithDeltas(final Dataset<Row> base, final Dataset<Row> deltas) {

        final StructType deltaType = this.deltaType;
        // Treat the base as a delta so we can use a single groupBy stage
        return latestDeltas(base
                .map(
                        (MapFunction<Row, Row>)row -> createDelta(deltaType, NO_SEQUENCE, null, null, row),
                        RowEncoder.apply(deltaType)
                ).union(deltas))
                .filter(functions.col(OPERATION).notEqual(UpsertOp.DELETE.name()))
                .select(functions.col(AFTER + ".*"))
                .select(baseColumns());
    }

    private Column[] deltaColumns() {

        final List<Column> cols = new ArrayList<>();
        cols.add(functions.col(SEQUENCE));
        cols.add(functions.col(OPERATION));
        basePartition.forEach(name -> cols.add(functions.col(name)));
        cols.add(functions.col(idColumn));
        cols.add(functions.col(BEFORE));
        cols.add(functions.col(AFTER));
        return cols.toArray(new Column[0]);
    }

    private Column[] baseColumns() {

        final List<Column> cols = new ArrayList<>();
        Arrays.stream(baseType.fieldNames()).forEach(name -> cols.add(functions.col(name)));
        return cols.toArray(new Column[0]);
    }

    public String sequence(final Instant timestamp) {

        return sequence(timestamp, UUID.randomUUID().toString());
    }

    public String sequence(final Instant timestamp, final String random) {

        return timestamp.toString().replaceAll("[:.Z\\-]", "") + "-" + random;
    }

    public void applyDelta(final Dataset<Tuple2<Row, Row>> changes) {

        applyDelta(changes, Instant.now());
    }

    protected void applyDelta(final Dataset<Tuple2<Row, Row>> changes, final Instant now) {

        final String sequence = sequence(now);
        applyDelta(changes, sequence);
    }

    public void applyDelta(final Dataset<Tuple2<Row, Row>> changes, final String sequence) {

        applyDelta(changes, sequence, row -> operation(row._1(), row._2()), Tuple2::_1, Tuple2::_2);
    }

    public <T> void applyDelta(final Dataset<T> changes, final String sequence, final MapFunction<T, UpsertOp> operation,
                               final MapFunction<T, Row> before, final MapFunction<T, Row> after) {

        final String deltaTable = deltaTableName();
        final URI deltaLocation = deltaLocation();

        final StructType deltaType = this.deltaType;
        final Dataset<Row> upsert = changes.map(
                (MapFunction<T, Row>) change -> createDelta(
                        deltaType,
                        sequence,
                        operation.call(change),
                        before.call(change),
                        after.call(change)
                ), RowEncoder.apply(deltaType));

        upsert.write().format(FORMAT.getSparkFormat())
                .mode(SaveMode.Append)
                .partitionBy(deltaPartition.toArray(new String[0]))
                .save(deltaLocation.toString());

        final SparkSession session = changes.sparkSession();
        final ExternalCatalog catalog = session.sharedState().externalCatalog();
        final Configuration configuration = session.sparkContext().hadoopConfiguration();

        final List<CatalogTablePartition> partitions = SparkCatalogUtils.findPartitions(catalog, configuration,
                database, deltaTable, deltaLocation, new SparkCatalogUtils.FindPartitionsStrategy.Default() {
                    @Override
                    public boolean include(final Map<String, String> spec, final String key, final String value) {

                        if(SEQUENCE.equals(key)) {
                            return sequence.equals(value);
                        } else {
                            return true;
                        }
                    }
                });

        catalog.createPartitions(database, deltaTable, ScalaUtils.asScalaSeq(partitions), false);
    }

    private static UpsertOp operation(final Row before, final Row after) {

        if(before == null) {
            if(after != null) {
                return UpsertOp.CREATE;
            } else {
                throw new IllegalStateException();
            }
        } else if(after != null) {
            return UpsertOp.UPDATE;
        } else {
            return UpsertOp.DELETE;
        }
    }

    private static Row createDelta(final StructType deltaType, final String sequence, final UpsertOp operation, final Row before, final Row after) {

        final StructField[] fields = deltaType.fields();
        final int size = fields.length;
        final Object[] data = new Object[size];
        // Sequence, operation
        data[0] = sequence;
        data[1] = Nullsafe.mapOrDefault(operation, UpsertOp::name, "");
        // Partition columns
        for(int i = 2; i != size - 3; ++i) {
            final String partition = fields[i].name();
            data[i] = SparkRowUtils.get(Nullsafe.orDefault(after, before), partition);
        }
        // Id
        data[size - 3] = SparkRowUtils.get(Nullsafe.orDefault(after, before), fields[size - 3].name());
        // Before, after
        data[size - 2] = SparkRowUtils.conform(before, fields[size - 2].dataType());
        data[size - 1] = SparkRowUtils.conform(after, fields[size - 1].dataType());
        return new GenericRowWithSchema(data, deltaType);
    }

    public void flattenDeltas(final SparkSession session) {

        final ExternalCatalog catalog = session.sharedState().externalCatalog();

        final Map<List<String>, CatalogTablePartition> current = new HashMap<>();
        final Map<List<String>, List<CatalogTablePartition>> delta = new HashMap<>();

        ScalaUtils.asJavaStream(catalog.listPartitions(database, baseTableName(), Option.empty())).forEach(part -> {
            final List<String> values = basePartitionValues(part);
            current.put(values, part);
        });
        ScalaUtils.asJavaStream(catalog.listPartitions(database, deltaTableName(), Option.empty())).forEach(part -> {
            final List<String> values = basePartitionValues(part);
            delta.compute(values, (k, v) -> Immutable.copyAdd(v, part));
        });

        final Map<List<String>, List<CatalogTablePartition>> append = new HashMap<>();
        final Map<List<String>, List<CatalogTablePartition>> merge = new HashMap<>();
        delta.forEach((values, deltas) -> {
            final CatalogTablePartition base = current.get(values);
            if(base == null) {
                append.put(values, deltas);
            } else {
                final Set<UpsertOp> ops = deltas.stream()
                        .map(v -> UpsertOp.valueOf(v.spec().get(OPERATION).get()))
                        .collect(Collectors.toSet());
                if(ops.contains(UpsertOp.DELETE) || ops.contains(UpsertOp.UPDATE)) {
                    merge.put(values, deltas);
                } else {
                    append.put(values, deltas);
                }
            }
        });

        log.warn("Flatten operations for {} are append: {} replace: {}", name, append.keySet(), merge.keySet());

        appendToBase(session, current, append);
        mergeToBase(session, current,  merge);

        final Set<scala.collection.immutable.Map<String, String>> drop = new HashSet<>();
        delta.values().forEach(parts -> parts.forEach(part -> drop.add(part.spec())));
        catalog.dropPartitions(database, deltaTableName(), ScalaUtils.asScalaSeq(drop), false, true, true);
    }

    private void appendToBase(final SparkSession session,
                              final Map<List<String>, CatalogTablePartition> current,
                              final Map<List<String>, List<CatalogTablePartition>> append) {

        if(!append.isEmpty()) {

            final ExternalCatalog catalog = session.sharedState().externalCatalog();

            final String[] appendLocations = partitionLocations(append.values().stream().flatMap(List::stream));
            final Dataset<Row> appendDeltas = latestDeltas(session.read()
                    .format(FORMAT.getSparkFormat())
                    .option("basePath", deltaLocation().toString())
                    .schema(deltaType)
                    .load(appendLocations));

            // Set sequence to the existing partitions to append into the same locations
            // if no existing partition we can use the initial NO_SEQUENCE value
            final Map<List<String>, String> sequences = new HashMap<>();
            append.keySet().forEach(spec -> {
                final CatalogTablePartition partition = current.get(spec);
                if(partition != null) {
                    sequences.put(spec, partition.spec().get(SEQUENCE).get());
                } else {
                    sequences.put(spec, NO_SEQUENCE);
                }
            });

            writeToBase(appendDeltas.select(functions.col(AFTER + ".*")), sequences);

            final Configuration configuration = session.sparkContext().hadoopConfiguration();
            repairBase(catalog, configuration);
            session.sql("REFRESH TABLE " + database + "." + baseTableName());
        }
    }

    private void mergeToBase(final SparkSession session,
                             final Map<List<String>, CatalogTablePartition> current,
                             final Map<List<String>, List<CatalogTablePartition>> merge) {

        if(!merge.isEmpty()) {

            final ExternalCatalog catalog = session.sharedState().externalCatalog();

            final String[] mergeDeltaLocations = partitionLocations(merge.values().stream().flatMap(List::stream));
            final Dataset<Row> mergeDeltas = latestDeltas(session.read()
                    .format(FORMAT.getSparkFormat()).option("basePath", deltaLocation().toString())
                    .schema(deltaType).load(mergeDeltaLocations));

            final List<CatalogTablePartition> mergeBasePartitions = new ArrayList<>();
            merge.keySet().forEach(k -> mergeBasePartitions.add(Nullsafe.require(current.get(k))));
            final String[] mergeBaseLocations = partitionLocations(mergeBasePartitions.stream());

            final Dataset<Row> mergeBase = session.read()
                    .format(FORMAT.getSparkFormat())
                    .option("basePath", baseLocation().toString())
                    .schema(baseType)
                    .load(mergeBaseLocations);

            // Use the largest sequence value in the new partitions
            final Map<List<String>, String> sequences = new HashMap<>();
            merge.forEach((spec, partitions) -> {
                final String sequence = partitions.stream().map(v -> v.spec().get(SEQUENCE).get())
                        .max(String::compareTo).orElseThrow(IllegalStateException::new);
                sequences.put(spec, sequence);
            });

            writeToBase(baseWithDeltas(mergeBase, mergeDeltas), sequences);

            final Configuration configuration = session.sparkContext().hadoopConfiguration();
            repairBase(catalog, configuration);

            session.sql("REFRESH TABLE " + database + "." + baseTableName());
        }
    }

    private void writeToBase(final Dataset<Row> output, final Map<List<String>, String> sequences) {

        final List<String> outputPartition = new ArrayList<>(basePartition);
        outputPartition.add(SEQUENCE);

        final StructField sequenceField = SparkRowUtils.field(SEQUENCE, DataTypes.StringType);
        final StructType outputType = SparkRowUtils.append(baseType, sequenceField);

        final List<String> basePartition = this.basePartition;

        output.select(baseColumns())
                .map((MapFunction<Row, Row>) row -> {

                    final List<String> partition = new ArrayList<>();
                    basePartition.forEach(p -> partition.add((String)SparkRowUtils.get(row, p)));
                    final String sequence = sequences.get(partition);
                    return SparkRowUtils.append(row, sequenceField, sequence);

                }, RowEncoder.apply(outputType))
                .write().format(FORMAT.getSparkFormat())
                .mode(SaveMode.Append)
                .partitionBy(outputPartition.toArray(new String[0]))
                .save(baseLocation().toString());

        // Create empty files for partitions that were not output
        final Configuration configuration = output.sparkSession().sparkContext().hadoopConfiguration();
        sequences.forEach((values, sequence) -> {
            final List<String> outputValues = Immutable.copyAdd(values, sequence);
            final List<Pair<String, String>> spec = Pair.zip(outputPartition.stream(), outputValues.stream())
                    .collect(Collectors.toList());
            final URI uri = SparkCatalogUtils.partitionLocation(baseLocation(), spec);
            final Path path = new Path(uri);
            try {
                final FileSystem fileSystem = path.getFileSystem(configuration);
                if(!fileSystem.exists(path)) {
                    log.warn("Emitting empty marker for partition {}", path);
                    try(final FSDataOutputStream os = fileSystem.create(path)) {
                        os.write("EMPTY".getBytes(StandardCharsets.UTF_8));
                        os.hsync();
                    }
                }
            } catch (final IOException e) {
                throw new IllegalStateException(e);
            }
        });
    }

    private static String[] partitionLocations(final Stream<CatalogTablePartition> partitions) {

        return partitions.map(CatalogTablePartition::location)
                .map(URI::toString)
                .toArray(String[]::new);
    }

    private List<String> basePartitionValues(final CatalogTablePartition partition) {

        final List<String> values = new ArrayList<>();
        for(final String name : basePartition) {
            final Option<String> opt = partition.spec().get(name);
            values.add(opt.isEmpty() ? null : opt.get());
        }
        return values;
    }

    public void provision(final SparkSession session) {

        final ExternalCatalog catalog = session.sharedState().externalCatalog();
        final Configuration configuration = session.sparkContext().hadoopConfiguration();
        provision(catalog, configuration);
    }

    public void repair(final SparkSession session) {

        final ExternalCatalog catalog = session.sharedState().externalCatalog();
        final Configuration configuration = session.sparkContext().hadoopConfiguration();
        repair(catalog, configuration);
    }

    public void provision(final ExternalCatalog catalog, final Configuration configuration) {

        if (!catalog.tableExists(database, baseTableName())) {
            repairBase(catalog, configuration);
        }
        if (!catalog.tableExists(database, deltaTableName())) {
            repairDelta(catalog, configuration);
        }
    }

    public void repair(final ExternalCatalog catalog, final Configuration configuration) {

        repairBase(catalog, configuration);
        repairDelta(catalog, configuration);
    }

    public void repairBase(final ExternalCatalog catalog, final Configuration configuration) {

        final String tableName = baseTableName();
        final URI location = baseLocation();
        SparkCatalogUtils.ensureTable(catalog, database, tableName, baseType, basePartition, FORMAT, location, TABLE_PROPERTIES);
        final List<CatalogTablePartition> partitions = SparkCatalogUtils.findPartitions(catalog, configuration,
                database, tableName, location, new BasePartitionStrategy());
        SparkCatalogUtils.syncTablePartitions(catalog, database, tableName, partitions, true);
    }

    private static class BasePartitionStrategy extends SparkCatalogUtils.FindPartitionsStrategy.Default {

        @Override
        public Optional<URI> location(final FileSystem fileSystem, final Path path) {

            return lastSequencePath(fileSystem, path);
        }
    }

    private static Optional<URI> lastSequencePath(final FileSystem fs, final Path path) {

        Path latest = null;
        String latestValue = null;
        try {
            for (final FileStatus status : fs.listStatus(path)) {
                final Path next = status.getPath();
                final String pathName = next.getName();
                final Pair<String, String> entry = SparkCatalogUtils.fromPartitionPathTerm(pathName);
                assert entry.getFirst().equals(SEQUENCE);
                final String nextValue = entry.getSecond();
                if (latestValue == null || nextValue.compareTo(latestValue) > 0) {
                    // Deleted partitions will be empty files
                    if (status.isDirectory()) {
                        latest = next;
                    } else {
                        log.debug("Found empty partition " + next);
                    }
                    latestValue = nextValue;
                }
            }
        } catch (final IOException e) {
            throw new IllegalStateException(e);
        }
        return Optional.ofNullable(latest).map(Path::toUri);
    }

    public void repairDelta(final ExternalCatalog catalog, final Configuration configuration) {

        final String tableName = deltaTableName();
        final URI location = deltaLocation();
        SparkCatalogUtils.ensureTable(catalog, database, tableName, deltaType, deltaPartition, FORMAT, location, TABLE_PROPERTIES);
    }

    public Source<Dataset<Row>> source(final SparkSession session) {

        return sink -> sink.accept(select(session));
    }

    public Query<Row> query(final SparkSession session) {

        return () -> select(session);
    }

    public Query<Row> queryDelta(final SparkSession session) {

        return () -> selectDelta(session);
    }

    public Query<Row> queryBase(final SparkSession session) {

        return () -> selectBase(session);
    }

    public Sink<Dataset<Tuple2<Row, Row>>> sink() {

        final String sequence = sequence(Instant.now());
        return input -> applyDelta(input, sequence);
    }
}
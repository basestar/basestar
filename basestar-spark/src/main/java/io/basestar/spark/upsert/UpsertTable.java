package io.basestar.spark.upsert;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.basestar.schema.Reserved;
import io.basestar.spark.query.Query;
import io.basestar.spark.source.Source;
import io.basestar.spark.util.*;
import io.basestar.util.Immutable;
import io.basestar.util.Nullsafe;
import io.basestar.util.Pair;
import io.basestar.util.Warnings;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.catalog.CatalogTablePartition;
import org.apache.spark.sql.catalyst.catalog.ExternalCatalog;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
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

    protected static final String EMPTY_PARTITION = "";

    private static final Format FORMAT = Format.PARQUET;

    private static final String BASE_PATH_OPTION = "basePath";

    protected final String database;

    protected final String name;

    protected final StructType baseType;

    protected final String idColumn;

    protected final String versionColumn;

    protected final List<String> basePartition;

    private final StructType deltaType;

    protected final List<String> deltaPartition;

    protected final String location;

    private volatile boolean autoProvisioned = false;

    @lombok.Builder(builderClassName = "Builder")
    protected UpsertTable(final String database, final String name, final StructType structType,
                          final String idColumn, final String versionColumn, final String location,
                          final List<String> partition) {

        this.database = Nullsafe.require(database);
        this.name = Nullsafe.require(name);
        this.idColumn = Nullsafe.require(idColumn);
        this.versionColumn = versionColumn;
        this.basePartition = Immutable.copy(partition);
        this.baseType = Nullsafe.require(structType);
        this.deltaType = createDeltaType(structType);
        this.deltaPartition = Immutable.copyAddAll(ImmutableList.of(SEQUENCE, OPERATION), basePartition);
        if(Nullsafe.require(location).endsWith("/")) {
            this.location = location;
        } else {
            this.location = location + "/";
        }
    }

    private static StructType createDeltaType(final StructType structType) {

        final List<StructField> fields = new ArrayList<>();
        fields.add(SparkRowUtils.field(SEQUENCE, DataTypes.StringType));
        fields.add(SparkRowUtils.field(OPERATION, DataTypes.StringType));
        fields.addAll(Arrays.asList(structType.fields()));
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

    public Dataset<Row> select(final SparkSession session) {

        if(hasDeltas(session)) {
            return baseWithDeltas(selectBase(session), selectLatestDelta(session));
        } else {
            return selectBase(session);
        }
    }

    private boolean hasDeltas(final SparkSession session) {

        final String tableName = deltaTableName();
        final ExternalCatalog catalog = session.sharedState().externalCatalog();
        if(SparkCatalogUtils.tableExists(catalog, database, tableName)) {
            return !catalog.listPartitions(database, tableName, Option.empty()).isEmpty();
        } else {
            return false;
        }
    }

    public static String sequence(final Instant timestamp) {

        return sequence(timestamp, UUID.randomUUID().toString());
    }

    public static String sequence(final Instant timestamp, final String random) {

        return timestamp.toString().replaceAll("[:.Z\\-]", "") + "-" + random;
    }

    public void applyChanges(final Dataset<Tuple2<Row, Row>> changes, final String sequence) {

        applyChanges(changes, sequence, v -> operation(v._1(), v._2()), v -> Nullsafe.orDefault(v._2(), v._1()));
    }

    public Set<Map<String, String>> deltaPartitions(final SparkSession session) {

        final ExternalCatalog catalog = session.sharedState().externalCatalog();
        return ScalaUtils.asJavaStream(catalog.listPartitions(database, deltaTableName(), Option.empty()))
            .map(v -> ScalaUtils.asJavaMap(v.spec()))
            .collect(Collectors.toSet());
    }

    @SuppressWarnings(Warnings.SHADOW_VARIABLES)
    public <T> void applyChanges(final Dataset<T> changes, final String sequence,
                                 final MapFunction<T, UpsertOp> op, final MapFunction<T, Row> row) {

        autoProvision(changes.sparkSession());
        final String deltaTable = deltaTableName();
        final URI deltaLocation = deltaLocation();

        final StructType deltaType = this.deltaType;
        final Dataset<Row> upsert = changes.map(
                SparkUtils.map(change -> createDelta(
                        deltaType,
                        sequence,
                        op.call(change),
                        row.call(change)
                )), RowEncoder.apply(deltaType));

        final SparkContext sc = changes.sparkSession().sparkContext();
        SparkUtils.withJobGroup(sc, "Delta " + name, () -> {

            upsert.write().format(FORMAT.getSparkFormat())
                    .mode(SaveMode.Append)
                    .partitionBy(deltaPartition.toArray(new String[0]))
                    .save(deltaLocation.toString());
        });

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

        if(!partitions.isEmpty()) {
            catalog.createPartitions(database, deltaTable, ScalaUtils.asScalaSeq(partitions), false);
        }
    }

    protected Dataset<Row> selectBase(final SparkSession session) {

        autoProvision(session);
        final String tableName = baseTableName();
        return session.sqlContext().read()
                .table(SparkCatalogUtils.escapeName(database, tableName))
                .select(baseColumns());
    }

    protected Dataset<Row> selectDelta(final SparkSession session) {

        autoProvision(session);
        final String tableName = deltaTableName();
        return session.sqlContext().read()
                .table(SparkCatalogUtils.escapeName(database, tableName))
                .select(deltaColumns());
    }

    protected Dataset<Row> selectDeltaAfter(final SparkSession session, final String sequence) {

        return selectDelta(session).filter(functions.col(SEQUENCE).gt(sequence));
    }

    protected Dataset<Row> selectLatestDelta(final SparkSession session) {

        return reduceDeltas(selectDelta(session));
    }

    protected Dataset<Row> reduceDeltas(final Dataset<Row> input) {

        final UpsertReducer reducer = new UpsertReducer(deltaType, versionColumn);
        return input.select(deltaColumns())
                .groupBy(functions.col(idColumn))
                .agg(reducer.apply(deltaColumns()).as("_2"))
                .select(functions.col("_2.*"))
                .select(deltaColumns());
    }

    @SuppressWarnings(Warnings.SHADOW_VARIABLES)
    protected Dataset<Row> baseWithDeltas(final Dataset<Row> base, final Dataset<Row> deltas) {

        final StructType deltaType = this.deltaType;
        // Treat the base as a delta so we can use a single groupBy stage
        return reduceDeltas(base
                .map(
                        SparkUtils.map(row -> createDelta(deltaType, EMPTY_PARTITION, null, row)),
                        RowEncoder.apply(deltaType)
                ).union(deltas))
                .filter(functions.col(OPERATION).notEqual(UpsertOp.DELETE.name()))
                .select(baseColumns());
    }

    private Column[] deltaColumns() {

        final List<Column> cols = new ArrayList<>();
        cols.add(functions.col(SEQUENCE));
        cols.add(functions.col(OPERATION));
        cols.addAll(Arrays.asList(baseColumns()));
        return cols.toArray(new Column[0]);
    }

    private Column[] baseColumns() {

        final List<Column> cols = new ArrayList<>();
        Arrays.stream(baseType.fieldNames()).forEach(n -> cols.add(functions.col(n)));
        return cols.toArray(new Column[0]);
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

    protected static Row createDelta(final StructType deltaType, final String sequence, final UpsertOp op, final Row row) {

        final StructField[] fields = deltaType.fields();
        final int size = fields.length;
        final Object[] data = new Object[size];
        // Sequence, operation
        data[0] = sequence;
        data[1] = Nullsafe.mapOrDefault(op, UpsertOp::name, EMPTY_PARTITION);
        // Data columns
        for(int i = 2; i != size; ++i) {
            final StructField field = fields[i];
            data[i] = SparkRowUtils.conform(SparkRowUtils.get(row, field.name()), field.dataType());
        }
        return new GenericRowWithSchema(data, deltaType);
    }

    public void squashDeltas(final SparkSession session) {

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

        squashAppend(session, current, append);
        squashMerge(session, current,  merge);

        final Set<scala.collection.immutable.Map<String, String>> drop = new HashSet<>();
        delta.values().forEach(parts -> parts.forEach(part -> drop.add(part.spec())));
        catalog.dropPartitions(database, deltaTableName(), ScalaUtils.asScalaSeq(drop), false, true, true);
    }

    private void squashAppend(final SparkSession session,
                              final Map<List<String>, CatalogTablePartition> current,
                              final Map<List<String>, List<CatalogTablePartition>> append) {

        if (!append.isEmpty()) {

            final String[] appendLocations = partitionLocations(append.values().stream().flatMap(List::stream));

            final SparkContext sc = session.sparkContext();
            SparkUtils.withJobGroup(sc, "Append " + name, () -> {

                final Dataset<Row> appendDeltas = reduceDeltas(session.read()
                        .format(FORMAT.getSparkFormat())
                        .option(BASE_PATH_OPTION, deltaLocation().toString())
                        .schema(deltaType)
                        .load(appendLocations));

                // Set sequence to the existing partitions to append into the same locations
                // if no existing partition we can use the initial EMPTY_PARTITION value
                final Map<List<String>, String> sequences = new HashMap<>();
                append.keySet().forEach(spec -> {
                    final CatalogTablePartition partition = current.get(spec);
                    if (partition != null) {
                        sequences.put(spec, partition.spec().get(SEQUENCE).get());
                    } else {
                        sequences.put(spec, EMPTY_PARTITION);
                    }
                });

                squash(appendDeltas, sequences);
            });
        }
    }

    private void squashMerge(final SparkSession session,
                             final Map<List<String>, CatalogTablePartition> current,
                             final Map<List<String>, List<CatalogTablePartition>> merge) {

        if (!merge.isEmpty()) {

            final String[] mergeDeltaLocations = partitionLocations(merge.values().stream().flatMap(List::stream));

            final SparkContext sc = session.sparkContext();
            SparkUtils.withJobGroup(sc, "Merge " + name, () -> {

                final Dataset<Row> mergeDeltas = reduceDeltas(session.read()
                        .format(FORMAT.getSparkFormat()).option(BASE_PATH_OPTION, deltaLocation().toString())
                        .schema(deltaType).load(mergeDeltaLocations));

                final List<CatalogTablePartition> mergeBasePartitions = new ArrayList<>();
                merge.keySet().forEach(k -> mergeBasePartitions.add(Nullsafe.require(current.get(k))));
                final String[] mergeBaseLocations = partitionLocations(mergeBasePartitions.stream());

                final Dataset<Row> mergeBase = session.read()
                        .format(FORMAT.getSparkFormat())
                        .option(BASE_PATH_OPTION, baseLocation().toString())
                        .schema(baseType)
                        .load(mergeBaseLocations);

                // Use the largest sequence value in the new partitions
                final Map<List<String>, String> sequences = new HashMap<>();
                merge.forEach((spec, partitions) -> {
                    final String sequence = partitions.stream().map(v -> v.spec().get(SEQUENCE).get())
                            .max(String::compareTo).orElseThrow(IllegalStateException::new);
                    sequences.put(spec, sequence);
                });

                squash(baseWithDeltas(mergeBase, mergeDeltas), sequences);
            });
        }
    }

    @SuppressWarnings(Warnings.SHADOW_VARIABLES)
    private void squash(final Dataset<Row> output, final Map<List<String>, String> sequences) {

        final List<String> outputPartition = new ArrayList<>(basePartition);
        outputPartition.add(SEQUENCE);

        final SparkSession session = output.sparkSession();

        final StructField sequenceField = SparkRowUtils.field(SEQUENCE, DataTypes.StringType);
        final StructType outputType = SparkRowUtils.append(baseType, sequenceField);

        final List<String> basePartition = this.basePartition;

        output.select(baseColumns())
                .map(SparkUtils.map(row -> {

                    final List<String> partition = new ArrayList<>();
                    basePartition.forEach(p -> partition.add(Nullsafe.mapOrDefault(SparkRowUtils.get(row, p), Object::toString, EMPTY_PARTITION)));
                    final String sequence = sequences.get(partition);
                    return SparkRowUtils.append(row, sequenceField, sequence);

                }), RowEncoder.apply(outputType))
                .write().format(FORMAT.getSparkFormat())
                .mode(SaveMode.Append)
                .partitionBy(outputPartition.toArray(new String[0]))
                .save(baseLocation().toString());

        // Create empty files for partitions that were not output
        final Configuration configuration = session.sparkContext().hadoopConfiguration();
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

        final ExternalCatalog catalog = session.sharedState().externalCatalog();
        final String baseTable = baseTableName();

        repairBase(catalog, configuration);
        session.sql("REFRESH TABLE " + SparkCatalogUtils.escapeName(database, baseTable));
        session.sqlContext().sql("ANALYZE TABLE " + SparkCatalogUtils.escapeName(database, baseTable) + " COMPUTE STATISTICS");
    }

    private static String[] partitionLocations(final Stream<CatalogTablePartition> partitions) {

        return partitions.map(CatalogTablePartition::location)
                .map(URI::toString)
                .toArray(String[]::new);
    }

    private List<String> basePartitionValues(final CatalogTablePartition partition) {

        final List<String> values = new ArrayList<>();
        for(final String field : basePartition) {
            final Option<String> opt = partition.spec().get(field);
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
            repairDelta(catalog);
        }
    }

    public void repair(final ExternalCatalog catalog, final Configuration configuration) {

        repairBase(catalog, configuration);
        repairDelta(catalog);
    }

    public void repairBase(final ExternalCatalog catalog, final Configuration configuration) {

        final String tableName = baseTableName();
        final URI baseLocation = baseLocation();
        SparkCatalogUtils.ensureTable(catalog, database, tableName, baseType, basePartition, FORMAT, baseLocation, TABLE_PROPERTIES);
        final List<CatalogTablePartition> partitions = SparkCatalogUtils.findPartitions(catalog, configuration,
                database, tableName, baseLocation, new BasePartitionStrategy());
        SparkCatalogUtils.syncTablePartitions(catalog, database, tableName, partitions, true);
    }

    private static class BasePartitionStrategy extends SparkCatalogUtils.FindPartitionsStrategy.Default {

        @Override
        public Optional<URI> location(final FileSystem fs, final Path path) {

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
    }

    public void repairDelta(final ExternalCatalog catalog) {

        final String tableName = deltaTableName();
        SparkCatalogUtils.ensureTable(catalog, database, tableName, deltaType, deltaPartition, FORMAT, deltaLocation(), TABLE_PROPERTIES);
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

    private void autoProvision(final SparkSession session) {

        synchronized (this) {
            if(!autoProvisioned) {
                provision(session);
                autoProvisioned = true;
            }
        }
    }
}
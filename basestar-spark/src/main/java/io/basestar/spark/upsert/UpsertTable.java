package io.basestar.spark.upsert;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
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
import java.io.UncheckedIOException;
import java.net.URI;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class UpsertTable {

    protected static final Map<String, String> TABLE_PROPERTIES = ImmutableMap.of("parquet.compress", "SNAPPY");

    protected static final String SEQUENCE = Reserved.PREFIX + "sequence";

    protected static final String OPERATION = Reserved.PREFIX + "operation";

    protected static final String DELETED = Reserved.PREFIX + "deleted";

    protected static final String EMPTY_PARTITION = "";

    private static final Format FORMAT = Format.PARQUET;

    private static final String BASE_PATH_OPTION = "basePath";

    private static final String MIN_SEQUENCE = sequence(Instant.parse("1970-01-01T00:00:00.000Z"), new UUID(0,0).toString());

    private static final String MAX_SEQUENCE = sequence(Instant.parse("9999-01-01T00:00:00.000Z"), new UUID(-1,-1).toString());

    protected final String database;

    protected final String name;

    protected final StructType inputType;

    protected final StructType baseType;

    protected final String idColumn;

    protected final String versionColumn;

    protected final List<String> basePartition;

    private final StructType deltaType;

    protected final List<String> deltaPartition;

    protected final String location;

    private final boolean deletedColumn;

    private final Set<Map<String, String>> partitionFilter;

    private volatile boolean autoProvisioned = false;

    @lombok.Builder(builderClassName = "Builder")
    protected UpsertTable(final String database, final String name, final StructType structType,
                          final String idColumn, final String versionColumn, final String location,
                          final List<String> partition, final boolean deletedColumn,
                          final boolean autoProvision, final Set<Map<String, String>> partitionFilter) {

        this.database = Nullsafe.require(database);
        this.name = Nullsafe.require(name);
        this.idColumn = Nullsafe.require(idColumn);
        this.versionColumn = versionColumn;
        this.basePartition = Immutable.list(partition);
        this.inputType = structType;
        this.baseType = createBaseType(structType, deletedColumn);
        this.deltaType = createDeltaType(structType);
        this.deltaPartition = Immutable.addAll(ImmutableList.of(SEQUENCE, OPERATION), basePartition);
        if(Nullsafe.require(location).endsWith("/")) {
            this.location = location;
        } else {
            this.location = location + "/";
        }
        this.deletedColumn = deletedColumn;
        this.partitionFilter = Immutable.set(partitionFilter);
        this.autoProvisioned = !autoProvision;
    }

    public UpsertTable withPartitionFilter(final Set<Map<String, String>> partitionFilter) {

        return new UpsertTable(database, name, inputType, idColumn, versionColumn, location, basePartition, deletedColumn, autoProvisioned, partitionFilter);
    }

    private static StructType createBaseType(final StructType structType, final boolean deletedColumn) {

        final List<StructField> fields = new ArrayList<>(Arrays.asList(structType.fields()));
        if(deletedColumn) {
            fields.add(SparkRowUtils.field(DELETED, DataTypes.BooleanType));
        }
        return DataTypes.createStructType(fields);
    }

    private static StructType createDeltaType(final StructType structType) {

        final List<StructField> fields = new ArrayList<>();
        fields.add(SparkRowUtils.field(SEQUENCE, DataTypes.StringType));
        fields.add(SparkRowUtils.field(OPERATION, DataTypes.StringType));
        fields.addAll(Arrays.asList(structType.fields()));
        return DataTypes.createStructType(fields);
    }

    public static String minSequence() {

        return MIN_SEQUENCE;
    }

    public static String maxSequence() {

        return MAX_SEQUENCE;
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

    public Dataset<Row> select(final SparkSession session, final boolean includeDeleted) {

        final boolean deletedColumn = this.deletedColumn;
        if(hasDeltas(session)) {
            final Dataset<Row> result = baseWithDeltas(selectBase(session, true), selectLatestDelta(session));
            if(includeDeleted) {
                return result;
            } else {
                return result.filter(SparkUtils.filter(row -> !isDeleted(row, deletedColumn))).select(inputColumns());
            }
        } else {
            return selectBase(session, includeDeleted);
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
        session.sql("REFRESH TABLE " + SparkCatalogUtils.escapeName(database, deltaTable));
    }

    private Dataset<Row> select(final SparkSession session, final StructType schema, final String tableName, final URI tableLocation) {

        if(partitionFilter.isEmpty()) {
            return session.sqlContext().read()
                    .table(SparkCatalogUtils.escapeName(database, tableName))
                    .select(Arrays.stream(schema.fieldNames()).map(functions::col).toArray(Column[]::new));
        } else {
            final ExternalCatalog catalog = session.sharedState().externalCatalog();
            final List<String> locations = new ArrayList<>();
            ScalaUtils.asJavaStream(catalog.listPartitions(database, tableName, Option.empty())).forEach(part -> {
                boolean matched = false;
                for(final Map<String, String> filter : partitionFilter) {
                    boolean entryMatched = true;
                    for(final Map.Entry<String, String> entry : filter.entrySet()) {
                        if(!entry.getValue().equals(part.spec().get(entry.getKey()).get())) {
                            entryMatched = false;
                            break;
                        }
                    }
                    if(entryMatched) {
                        matched = true;
                        break;
                    }
                }
                if(matched) {
                    locations.add(part.location().toString());
                }
            });
            return session.sqlContext().read()
                    .format(FORMAT.getSparkFormat())
                    .option(BASE_PATH_OPTION, tableLocation.toString())
                    .schema(schema)
                    .load(ScalaUtils.asScalaSeq(locations));
        }
    }

    protected Dataset<Row> selectBase(final SparkSession session, final boolean includeDeleted) {

        autoProvision(session);
        final boolean deletedColumn = this.deletedColumn;
        final Dataset<Row> result = select(session, baseType, baseTableName(), baseLocation());
        if(includeDeleted) {
            return result;
        } else {
            return result.filter(SparkUtils.filter(row -> !isDeleted(row, deletedColumn))).select(inputColumns());
        }
    }

    protected Dataset<Row> selectDelta(final SparkSession session) {

        autoProvision(session);
        return select(session, deltaType, deltaTableName(), deltaLocation());
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
                .groupBy(groupColumns())
                .agg(reducer.apply(deltaColumns()).as("_2"))
                .select(functions.col("_2.*"))
                .select(deltaColumns());
    }

    private Column[] groupColumns() {

        return Stream.concat(basePartition.stream(), Stream.of(idColumn))
                .map(functions::col).toArray(Column[]::new);
    }

    @SuppressWarnings(Warnings.SHADOW_VARIABLES)
    protected Dataset<Row> baseWithDeltas(final Dataset<Row> base, final Dataset<Row> deltas) {

        final boolean deletedColumn = this.deletedColumn;
        final StructType deltaType = this.deltaType;
        // Treat the base as a delta so we can use a single groupBy stage
        return reduceDeltas(base
                .map(
                        SparkUtils.map(row -> createDelta(deltaType, EMPTY_PARTITION, isDeleted(row, deletedColumn) ? UpsertOp.DELETE : null, row)),
                        RowEncoder.apply(deltaType)
                ).union(deltas))
                .select(inputColumnsWithDeleted());
    }

    private static boolean isDeleted(final Row row, final boolean deletedColumn) {

        if(deletedColumn) {
            return Nullsafe.orDefault((Boolean) SparkRowUtils.get(row, DELETED));
        } else {
            return false;
        }
    }

    private Column[] inputColumns() {

        final List<Column> cols = new ArrayList<>();
        Arrays.stream(inputType.fieldNames()).forEach(n -> cols.add(functions.col(n)));
        return cols.toArray(new Column[0]);
    }

    private Column[] inputColumnsWithDeleted() {

        final List<Column> cols = new ArrayList<>();
        Arrays.stream(inputType.fieldNames()).forEach(n -> cols.add(functions.col(n)));
        if(deletedColumn) {
            cols.add(functions.col(OPERATION).equalTo(UpsertOp.DELETE.name()).as(DELETED));
        }
        return cols.toArray(new Column[0]);
    }

    private Column[] deltaColumns() {

        final List<Column> cols = new ArrayList<>();
        Arrays.stream(deltaType.fieldNames()).forEach(n -> cols.add(functions.col(n)));
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
            delta.compute(values, (k, v) -> Immutable.add(v, part));
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

        final String deltaTable = deltaTableName();
        final Set<scala.collection.immutable.Map<String, String>> drop = new HashSet<>();
        delta.values().forEach(parts -> parts.forEach(part -> drop.add(part.spec())));
        catalog.dropPartitions(database, deltaTable, ScalaUtils.asScalaSeq(drop), false, true, true);
        session.sql("REFRESH TABLE " + SparkCatalogUtils.escapeName(database, deltaTable));
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
                        .load(appendLocations))
                        .select(inputColumnsWithDeleted());

                // Set sequence to the existing partition to append into the same locations
                // if no existing partition we can use the initial EMPTY_PARTITION value
                final Map<List<String>, String> sequences = new HashMap<>();
                append.keySet().forEach(spec -> {
                    final CatalogTablePartition partition = current.get(spec);
                    if (partition != null) {
                        final URI location = partition.storage().locationUri().get();
                        final String term = Iterables.getLast(Splitter.on("/").omitEmptyStrings().split(location.toString()));
                        final Pair<String, String> sequence = SparkCatalogUtils.fromPartitionPathTerm(term);
                        assert SEQUENCE.equals(sequence.getFirst());
                        sequences.put(spec, sequence.getSecond());
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

        final SparkSession session = output.sparkSession();
        final ExternalCatalog catalog = session.sharedState().externalCatalog();

        final List<String> outputPartition = new ArrayList<>(basePartition);
        outputPartition.add(SEQUENCE);

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

        final List<CatalogTablePartition> syncPartitions = new ArrayList<>();
        for(final List<String> partition : sequences.keySet()) {
            final String sequence = sequences.get(partition);
            final List<String> outputValues = Immutable.add(partition, sequence);
            final List<Pair<String, String>> outputSpec = Pair.zip(outputPartition.stream(), outputValues.stream())
                    .collect(Collectors.toList());
            final URI uri = SparkCatalogUtils.partitionLocation(baseLocation(), outputSpec);
            final Map<String, String> spec = outputSpec.stream().collect(Collectors.toMap(Pair::getFirst, Pair::getSecond));
            spec.remove(SEQUENCE);
            syncPartitions.add(SparkCatalogUtils.partition(spec, FORMAT, uri));
        }

        // Create empty files for partitions that were not output
//        final Configuration configuration = session.sparkContext().hadoopConfiguration();
//        sequences.forEach((values, sequence) -> {
//            final List<String> outputValues = Immutable.add(values, sequence);
//            final List<Pair<String, String>> spec = Pair.zip(outputPartition.stream(), outputValues.stream())
//                    .collect(Collectors.toList());
//            final URI uri = SparkCatalogUtils.partitionLocation(baseLocation(), spec);
//            final Path path = new Path(uri);
//            try {
//                final FileSystem fileSystem = path.getFileSystem(configuration);
//                if(!fileSystem.exists(path)) {
//                    log.warn("Emitting empty marker for partition {}", path);
//                    try(final FSDataOutputStream os = fileSystem.create(path)) {
//                        os.write("EMPTY".getBytes(StandardCharsets.UTF_8));
//                        os.hsync();
//                    }
//                }
//            } catch (final IOException e) {
//                throw new IllegalStateException(e);
//            }
//        });

        final String baseTable = baseTableName();

        SparkCatalogUtils.syncTablePartitions(catalog, database, baseTable, syncPartitions, SparkCatalogUtils.MissingPartitions.SKIP);
        session.sql("REFRESH TABLE " + SparkCatalogUtils.escapeName(database, baseTable));
        session.sql("ANALYZE TABLE " + SparkCatalogUtils.escapeName(database, baseTable) + " COMPUTE STATISTICS");
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
        if (!catalog.tableExists(database, baseTableName())) {
            repairBase(session);
        }
        if (!catalog.tableExists(database, deltaTableName())) {
            repairDelta(session);
        }
    }

    public void repair(final SparkSession session) {

        repairBase(session);
        repairDelta(session);
    }

    public void repairBase(final SparkSession session) {

        final ExternalCatalog catalog = session.sharedState().externalCatalog();
        final Configuration configuration = session.sparkContext().hadoopConfiguration();
        final String tableName = baseTableName();
        final URI baseLocation = baseLocation();
        SparkCatalogUtils.ensureTable(catalog, database, tableName, baseType, basePartition, FORMAT, baseLocation, TABLE_PROPERTIES);
        final List<CatalogTablePartition> partitions = SparkCatalogUtils.findPartitions(catalog, configuration,
                database, tableName, baseLocation, new BasePartitionStrategy());
        SparkCatalogUtils.syncTablePartitions(catalog, database, tableName, partitions, SparkCatalogUtils.MissingPartitions.DROP_AND_RETAIN);
        session.sql("REFRESH TABLE " + SparkCatalogUtils.escapeName(database, tableName));
    }

    public void repairDelta(final SparkSession session) {

        final ExternalCatalog catalog = session.sharedState().externalCatalog();
        final String tableName = deltaTableName();
        SparkCatalogUtils.ensureTable(catalog, database, tableName, deltaType, deltaPartition, FORMAT, deltaLocation(), TABLE_PROPERTIES);
        session.sql("REFRESH TABLE " + SparkCatalogUtils.escapeName(database, tableName));
    }

    public List<SequenceEntry> getSequence(final SparkSession session) {

        return getSequence(session, MIN_SEQUENCE, MAX_SEQUENCE);
    }

    public List<SequenceEntry> getSequence(final SparkSession session, final String afterSequence) {

        return getSequence(session, afterSequence, MAX_SEQUENCE);
    }

    public List<SequenceEntry> getSequence(final SparkSession session, final String afterSequence, final String beforeSequence) {

        autoProvision(session);
        final ExternalCatalog catalog = session.sharedState().externalCatalog();
        final Configuration configuration = session.sparkContext().hadoopConfiguration();
        final List<CatalogTablePartition> partitions = deltaPartitionsBetween(catalog, configuration, afterSequence, beforeSequence);
        final SortedMap<String, Map<UpsertOp, Set<Map<String, String>>>> entries = new TreeMap<>();
        partitions.forEach(partition -> {
            final scala.collection.Map<String, String> spec = partition.spec();
            final String sequence = spec.get(SEQUENCE).get();
            final UpsertOp operation = UpsertOp.valueOf(spec.get(OPERATION).get());
            final Map<String, String> values = basePartition.stream().collect(Collectors.toMap(
                    e -> e,
                    e -> spec.get(e).get()
            ));
            entries.computeIfAbsent(sequence, ignored -> new HashMap<>())
                    .computeIfAbsent(operation, ignored -> new HashSet<>())
                    .add(values);
        });
        return entries.entrySet().stream()
                .map(e -> new SequenceEntry(e.getKey(), e.getValue()))
                .collect(Collectors.toList());
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
                            latest = null;
                            log.debug("Found empty partition " + next);
                        }
                        latestValue = nextValue;
                    }
                }
            } catch (final IOException e) {
                throw new UncheckedIOException(e);
            }
            return Optional.ofNullable(latest).map(Path::toUri);
        }
    }

    private List<CatalogTablePartition> deltaPartitionsBetween(final ExternalCatalog catalog, final Configuration configuration, final String afterSequence, final String beforeSequence) {


        final String tableName = deltaTableName();
        final URI location = deltaLocation();
        return SparkCatalogUtils.findPartitions(catalog, configuration, database, tableName, location, new SparkCatalogUtils.FindPartitionsStrategy() {
            @Override
            public boolean include(final Map<String, String> spec, final String key, final String value) {

                if(key.equals(SEQUENCE)) {
                    return value.compareTo(afterSequence) > 0 && value.compareTo(beforeSequence) < 0;
                } else {
                    return true;
                }
            }

            @Override
            public Optional<URI> location(final FileSystem fileSystem, final Path path) {

                return Optional.of(path.toUri());
            }
        });
    }

    public void replayDeltas(final SparkSession session, final String afterSequence, final String beforeSequence) {

        autoProvision(session);
        final ExternalCatalog catalog = session.sharedState().externalCatalog();
        final Configuration configuration = session.sparkContext().hadoopConfiguration();
        final String tableName = deltaTableName();
        final List<CatalogTablePartition> partitions = deltaPartitionsBetween(catalog, configuration, afterSequence, beforeSequence);
        SparkCatalogUtils.syncTablePartitions(catalog, database, tableName, partitions, SparkCatalogUtils.MissingPartitions.SKIP);
    }

    public Source<Dataset<Row>> source(final SparkSession session) {

        return sink -> sink.accept(select(session, false));
    }

    public Query<Row> query(final SparkSession session) {

        return () -> select(session, false);
    }

    public Query<Row> queryDelta(final SparkSession session) {

        return () -> selectDelta(session);
    }

    public Query<Row> queryBase(final SparkSession session) {

        return () -> selectBase(session, false);
    }

    public void dropBase(final SparkSession session, final boolean purge) {

        drop(session, database, baseTableName(), baseLocation(), purge);
    }

    public void dropDeltas(final SparkSession session, final boolean purge) {

        drop(session, database, deltaTableName(), deltaLocation(), purge);
    }

    private static void drop(final SparkSession session, final String database, final String tableName, final URI location, final boolean purge) {

        final ExternalCatalog catalog = session.sharedState().externalCatalog();
        final Configuration configuration = session.sparkContext().hadoopConfiguration();
        catalog.dropTable(database, tableName, true, purge);
        if(purge) {
            try {
                final Path path = new Path(location);
                final FileSystem fs = path.getFileSystem(configuration);
                fs.delete(path, true);
            } catch (final IOException e) {
                log.error("Failed to purge " + location, e);
            }
        }
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
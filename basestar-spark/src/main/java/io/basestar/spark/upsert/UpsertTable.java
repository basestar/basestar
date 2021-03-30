package io.basestar.spark.upsert;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.basestar.schema.Reserved;
import io.basestar.spark.query.Query;
import io.basestar.spark.source.Source;
import io.basestar.spark.util.*;
import io.basestar.util.*;
import lombok.AccessLevel;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.*;
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
import java.io.UncheckedIOException;
import java.net.URI;
import java.time.Instant;
import java.util.*;
import java.util.function.Function;
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

    protected final Name tableName;

    protected final StructType schema;

    protected final StructType baseType;

    protected final String idColumn;

    protected final String versionColumn;

    protected final List<String> basePartition;

    private final StructType deltaType;

    protected final List<String> deltaPartition;

    protected final URI baseLocation;

    protected final URI deltaLocation;

    protected final UpsertState state;

    private final boolean deletedColumn;

    private final Set<Map<String, String>> partitionFilter;

    private volatile boolean provisioned;

    @lombok.Builder(builderClassName = "Builder", access = AccessLevel.PUBLIC)
    protected UpsertTable(final Name tableName, final StructType schema,
                          final String idColumn, final String versionColumn,
                          final URI baseLocation, final URI deltaLocation, final UpsertState state,
                          final List<String> partition, final boolean deletedColumn,
                          final boolean provisioned, final Set<Map<String, String>> partitionFilter) {

        this.tableName = tableName;
        if(tableName != null) {
            if(tableName.size() < 2) {
                throw new IllegalStateException("Table name " + tableName + " is not valid");
            }
        }
        this.idColumn = Nullsafe.require(idColumn);
        this.versionColumn = versionColumn;
        this.basePartition = Immutable.list(partition);
        this.schema = schema;
        this.baseType = createBaseType(schema, deletedColumn);
        this.deltaType = createDeltaType(schema);
        this.deltaPartition = Immutable.addAll(ImmutableList.of(SEQUENCE, OPERATION), basePartition);
        this.baseLocation = Nullsafe.require(baseLocation);
        this.deltaLocation = Nullsafe.require(deltaLocation);
        this.state = Nullsafe.require(state);
        this.deletedColumn = deletedColumn;
        this.partitionFilter = Immutable.set(partitionFilter);
        this.provisioned = provisioned;
    }

    public UpsertTable withPartitionFilter(final Set<Map<String, String>> partitionFilter) {

        return new UpsertTable(tableName, schema, idColumn, versionColumn, baseLocation, deltaLocation, state, basePartition, deletedColumn, provisioned, partitionFilter);
    }

    public UpsertTable withSchema(final StructType schema) {

        return new UpsertTable(tableName, schema, idColumn, versionColumn, baseLocation, deltaLocation, state, basePartition, deletedColumn, provisioned, partitionFilter);
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

//    protected String baseTableName() {
//
//        return name + "_base";
//    }
//
//    protected static URI baseLocation(final String location) {
//
//        return URI.create(location(location) + "base");
//    }
//
//    protected URI baseLocation() {
//
//        return baseLocation(location);
//    }
//
//    protected String deltaTableName() {
//
//        return name + "_delta";
//    }
//
//    protected static URI deltaLocation(final String location) {
//
//        return URI.create(location(location) + "delta");
//    }
//
//    protected URI deltaLocation() {
//
//        return deltaLocation(location);
//    }
//
//    protected static URI stateLocation(final String location) {
//
//        return URI.create(location(location) + "state.json");
//    }
//
//    protected URI stateLocation() {
//
//        return stateLocation(location);
//    }

    public Dataset<Row> select(final SparkSession session, final boolean includeDeleted) {

        final boolean deletedColumn = this.deletedColumn;
        if(hasDeltas(session)) {
            final Dataset<Row> result = baseWithDeltas(selectBase(session, true), selectDelta(session));
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

        final DeltaState state = deltaState(session);
        return !state.getSequences().isEmpty();
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
                                 final MapFunction<T, UpsertOp> ops, final MapFunction<T, Row> rows) {

        autoProvision(changes.sparkSession());

        final StructType deltaType = this.deltaType;
        final Dataset<Row> upsert = changes.map(
                SparkUtils.map(change -> createDelta(
                        deltaType,
                        sequence,
                        ops.call(change),
                        rows.call(change)
                )), RowEncoder.apply(deltaType));

        final SparkContext sc = changes.sparkSession().sparkContext();
        SparkUtils.withJobGroup(sc, "Delta " + tableName, () -> {

            upsert.write().format(FORMAT.getSparkFormat())
                    .mode(SaveMode.Append)
                    .partitionBy(deltaPartition.toArray(new String[0]))
                    .save(deltaLocation.toString());
        });

        final SparkSession session = changes.sparkSession();
        final Configuration configuration = session.sparkContext().hadoopConfiguration();

        final List<CatalogTablePartition> partitions = SparkCatalogUtils.findPartitions(configuration, deltaLocation, deltaPartition, new SparkCatalogUtils.FindPartitionsStrategy.Default() {
                    @Override
                    public boolean include(final Map<String, String> spec, final String key, final String value) {

                        if(SEQUENCE.equals(key)) {
                            return sequence.equals(value);
                        } else {
                            return true;
                        }
                    }
                });

        final Map<UpsertOp, Set<Map<String, String>>> merge = new HashMap<>();
        partitions.forEach(partition -> {

            final Map<String, String> spec = ScalaUtils.asJavaMap(partition.spec());
            final UpsertOp op = UpsertOp.valueOf(spec.get(OPERATION));
            final Map<String, String> values = basePartition.stream().collect(Collectors.toMap(k -> k, spec::get));
            merge.computeIfAbsent(op, ignored -> new HashSet<>()).add(values);
        });

        if(!merge.isEmpty()) {
            log.info("Applying delta changes {} {}", tableName, merge);
            updateDeltaState(session, state -> state.mergeSequence(sequence, merge));
        }
    }

//    private Dataset<Row> select(final SparkSession session, final StructType schema, final String tableName, final URI tableLocation) {
//
//        if(partitionFilter.isEmpty()) {
//            return session.sqlContext().read()
//                    .table(SparkCatalogUtils.escapeName(database, tableName))
//                    .select(Arrays.stream(schema.fieldNames()).map(functions::col).toArray(Column[]::new));
//        } else {
//            final ExternalCatalog catalog = session.sharedState().externalCatalog();
//            final List<String> locations = filteredPartitions(catalog, baseTableName(), partitionFilter)
//                    .stream().map(v -> v.location().toString()).collect(Collectors.toList());
//            return session.sqlContext().read()
//                    .format(FORMAT.getSparkFormat())
//                    .option(BASE_PATH_OPTION, tableLocation.toString())
//                    .schema(schema)
//                    .load(ScalaUtils.asScalaSeq(locations));
//        }
//    }
//
//    private List<CatalogTablePartition> filteredPartitions(final ExternalCatalog catalog, final String tableName, final Set<Map<String, String>> partitionFilter) {
//
//        final List<CatalogTablePartition> locations = new ArrayList<>();
//        ScalaUtils.asJavaStream(catalog.listPartitions(database, tableName, Option.empty())).forEach(part -> {
//            boolean matched = (partitionFilter == null);
//            if(!matched) {
//                for (final Map<String, String> filter : partitionFilter) {
//                    boolean entryMatched = true;
//                    for (final Map.Entry<String, String> entry : filter.entrySet()) {
//                        if (!entry.getValue().equals(part.spec().get(entry.getKey()).get())) {
//                            entryMatched = false;
//                            break;
//                        }
//                    }
//                    if (entryMatched) {
//                        matched = true;
//                        break;
//                    }
//                }
//            }
//            if(matched) {
//                locations.add(part);
//            }
//        });
//        return locations;
//    }

//    public UpsertTable copy(final SparkSession session, final String database, final String name, final String location) {
//
//        return copy(session, database, name, location, (String)null);
//    }
//
    public UpsertTable copy(final SparkSession session, final Name newTableName, final URI newBaseLocation, final URI newDeltaLocation, final UpsertState newState) {

        return copy(session, newTableName, newBaseLocation, newDeltaLocation, newState, null);
    }

    public UpsertTable copy(final SparkSession session, final Name newTableName, final URI newBaseLocation, final URI newDeltaLocation, final UpsertState newState, final String beforeSequence) {

        Dataset<Row> deltas = session.sqlContext().read()
                .schema(deltaType)
                .parquet(deltaLocation.toString());
        if(beforeSequence != null) {
            deltas = deltas.filter(functions.col(SEQUENCE).lt(beforeSequence));
        }
        deltas.select(deltaColumns())
                .write()
                .partitionBy(deltaPartition.toArray(new String[0]))
                .parquet(newDeltaLocation.toString());

        final boolean deletedColumn = this.deletedColumn;

        final List<Column> outputCols = Lists.newArrayList(inputColumnsWithDeleted());
        outputCols.add(functions.lit("").as(SEQUENCE));

        final List<String> outputPartition = new ArrayList<>(basePartition);
        outputPartition.add(SEQUENCE);

        final Dataset<Row> base = reduceDelta(deltas).select(outputCols.toArray(new Column[0]))
                .filter(SparkUtils.filter(row -> !isDeleted(row, deletedColumn)));

        base.write()
                .partitionBy(outputPartition.toArray(new String[0]))
                .parquet(newBaseLocation.toString());

        final UpsertTable copy = new UpsertTable(newTableName, schema, idColumn, versionColumn, newBaseLocation, newDeltaLocation, newState, basePartition, deletedColumn, false, null);
        copy.repair(session);
        return copy;
    }

    protected Dataset<Row> selectBase(final SparkSession session, final boolean includeDeleted) {

        autoProvision(session);

        final BaseState baseState = baseState(session);
        final String[] locations = baseState.locations(baseLocation, basePartition, partitionFilter)
                .map(URI::toString).toArray(String[]::new);

        final Dataset<Row> result;
        if(locations.length == 0) {
            result = session.createDataFrame(ImmutableList.of(), baseType);
        } else {
            result = session.sqlContext().read()
                    .format(FORMAT.getSparkFormat())
                    .option(BASE_PATH_OPTION, baseLocation.toString())
                    .schema(baseType)
                    .load(locations);
        }

        if(includeDeleted) {
            return result;
        } else {
            final boolean deletedColumn = this.deletedColumn;
            return result.filter(SparkUtils.filter(row -> !isDeleted(row, deletedColumn))).select(inputColumns());
        }
    }

    private Dataset<Row> selectDelta(final SparkSession session, final String[] locations) {

        if(locations.length == 0) {
            return session.createDataFrame(ImmutableList.of(), deltaType);
        } else {
            return session.sqlContext().read()
                    .format(FORMAT.getSparkFormat())
                    .option(BASE_PATH_OPTION, deltaLocation.toString())
                    .schema(deltaType)
                    .load(locations)
                    .select(deltaColumns());
        }
    }

    public Dataset<Row> selectDelta(final SparkSession session) {

        autoProvision(session);

        final DeltaState deltaState = deltaState(session);
        final String[] locations = deltaState.locations(deltaLocation, basePartition, partitionFilter)
                .map(URI::toString).toArray(String[]::new);

        return selectDelta(session, locations);
    }

    public Dataset<Row> selectLatestDelta(final SparkSession session) {

        return reduceDelta(selectDelta(session));
    }

    public Dataset<Row> selectSequenceAfter(final SparkSession session, final String sequence) {

        final String[] locations = sequenceAfter(session, sequence).stream()
                .flatMap(seq -> seq.locations(deltaLocation, basePartition, partitionFilter))
                .map(URI::toString).toArray(String[]::new);

        return selectDelta(session, locations);
    }

    protected Dataset<Row> reduceDelta(final Dataset<Row> input) {

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
        return reduceDelta(base
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
        Arrays.stream(schema.fieldNames()).forEach(n -> cols.add(functions.col(n)));
        return cols.toArray(new Column[0]);
    }

    private Column[] inputColumnsWithDeleted() {

        final List<Column> cols = new ArrayList<>();
        Arrays.stream(schema.fieldNames()).forEach(n -> cols.add(functions.col(n)));
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

        autoProvision(session);

        final Map<List<String>, CatalogTablePartition> current = new HashMap<>();
        final Map<List<String>, List<CatalogTablePartition>> delta = new HashMap<>();

        final BaseState baseState = baseState(session);
        final DeltaState deltaState = deltaState(session);

        final Set<String> sequences = deltaState.getSequences().stream()
                .map(DeltaState.Sequence::getSequence).collect(Collectors.toSet());

        baseState.toCatalogPartitions(baseLocation, basePartition).forEach(part -> {
            final List<String> values = basePartitionValues(part);
            current.put(values, part);
        });
        deltaState.toCatalogPartitions(deltaLocation, basePartition).forEach(part -> {
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

        log.warn("Flatten operations for {} are append: {} replace: {}", tableName, append.keySet(), merge.keySet());

        squashAppend(session, current, append);
        squashMerge(session, current,  merge);

        updateDeltaState(session, state -> state.dropSequences(sequences));

        refreshTable(session);
    }

    private void squashAppend(final SparkSession session,
                              final Map<List<String>, CatalogTablePartition> current,
                              final Map<List<String>, List<CatalogTablePartition>> append) {

        if (!append.isEmpty()) {

            final String[] appendLocations = partitionLocations(append.values().stream().flatMap(List::stream));

            final SparkContext sc = session.sparkContext();
            SparkUtils.withJobGroup(sc, "Append " + tableName, () -> {

                final Dataset<Row> appendDeltas = reduceDelta(session.read()
                        .format(FORMAT.getSparkFormat())
                        .option(BASE_PATH_OPTION, deltaLocation.toString())
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
            SparkUtils.withJobGroup(sc, "Merge " + tableName, () -> {

                final Dataset<Row> mergeDeltas = reduceDelta(session.read()
                        .format(FORMAT.getSparkFormat()).option(BASE_PATH_OPTION, deltaLocation.toString())
                        .schema(deltaType).load(mergeDeltaLocations));

                final List<CatalogTablePartition> mergeBasePartitions = new ArrayList<>();
                merge.keySet().forEach(k -> mergeBasePartitions.add(Nullsafe.require(current.get(k))));
                final String[] mergeBaseLocations = partitionLocations(mergeBasePartitions.stream());

                final Dataset<Row> mergeBase = session.read()
                        .format(FORMAT.getSparkFormat())
                        .option(BASE_PATH_OPTION, baseLocation.toString())
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
                .save(baseLocation.toString());

        final Map<Map<String, String>, String> syncPartitions = new HashMap<>();
        for(final List<String> partition : sequences.keySet()) {
            final String sequence = sequences.get(partition);
            final List<String> outputValues = Immutable.add(partition, sequence);
            final List<Pair<String, String>> outputSpec = Pair.zip(outputPartition.stream(), outputValues.stream())
                    .collect(Collectors.toList());
            final Map<String, String> spec = outputSpec.stream().collect(Collectors.toMap(Pair::getFirst, Pair::getSecond));
            spec.remove(SEQUENCE);
            syncPartitions.put(spec, sequence);
        }

        updateBaseState(session, state -> state.mergePartitions(syncPartitions));

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


    }

    public void refreshTable(final SparkSession session) {

        if(tableName != null) {

            autoProvision(session);

            final String database = tableName.first();
            final String table = tableName.withoutFirst().toString("_");

            final BaseState baseState = baseState(session);
            final ExternalCatalog catalog = session.sharedState().externalCatalog();

            final List<CatalogTablePartition> partitions = baseState.toCatalogPartitions(baseLocation, basePartition);

            SparkCatalogUtils.syncTablePartitions(catalog, database, table, partitions, SparkCatalogUtils.MissingPartitions.DROP_AND_RETAIN);
            session.sql("REFRESH TABLE " + SparkCatalogUtils.escapeName(database, table));
            session.sql("ANALYZE TABLE " + SparkCatalogUtils.escapeName(database, table) + " COMPUTE STATISTICS");
        }
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

        if(!state.hasState(session, BaseState.KEY)) {
            repairBase(session);
            repairDelta(session);
        } else {
            provisionBase(session);
            provisionDelta(session);
        }
    }

    public void repair(final SparkSession session) {

        repairBase(session);
        repairDelta(session);
    }

    public void provisionBase(final SparkSession session) {

        state.provisionState(session, BaseState.KEY, BaseState.class, () -> new BaseState(Immutable.list()));
        if(tableName != null) {
            final ExternalCatalog catalog = session.sharedState().externalCatalog();
            final String database = tableName.first();
            final String table = tableName.withoutFirst().toString("_");
            SparkCatalogUtils.ensureTable(catalog, database, table, baseType, basePartition, FORMAT, baseLocation, TABLE_PROPERTIES);
        }
    }

    public void repairBase(final SparkSession session) {

        provisionBase(session);

        final Configuration configuration = session.sparkContext().hadoopConfiguration();

        final Map<Map<String, String>, String> partitions = new HashMap<>();
        SparkCatalogUtils.findPartitions(configuration,
                baseLocation, basePartition, new BasePartitionStrategy()).forEach(partition -> {

                final Map<String, String> values = basePartition.stream().collect(Collectors.toMap(k -> k, k -> partition.spec().get(k).get()));
                final String location = partition.location().toString();
                final String prefix = Text.ensureSuffix(baseLocation.toString(), "/");
                assert location.startsWith(prefix);
                final String term = location.substring(prefix.length()).split("/")[basePartition.size()];
                final Pair<String, String> parts = SparkCatalogUtils.fromPartitionPathTerm(term);
                assert parts.getFirst().equals(SEQUENCE);
                final String sequence = parts.getSecond();
                partitions.put(values, sequence);
        });

        updateBaseState(session, current -> current.mergePartitions(partitions));

        refreshTable(session);
    }

    public void provisionDelta(final SparkSession session) {

        state.provisionState(session, DeltaState.KEY, DeltaState.class, () -> new DeltaState(Immutable.list()));
    }

    public void repairDelta(final SparkSession session) {

        provisionDelta(session);
    }

//    public List<SequenceEntry> getSequence(final SparkSession session) {
//
//        return getSequence(session, MIN_SEQUENCE, MAX_SEQUENCE);
//    }
//
//    public List<SequenceEntry> getSequence(final SparkSession session, final String afterSequence) {
//
//        return getSequence(session, afterSequence, MAX_SEQUENCE);
//    }
//
//    public List<SequenceEntry> getSequence(final SparkSession session, final String afterSequence, final String beforeSequence) {
//
//        autoProvision(session);
//        final ExternalCatalog catalog = session.sharedState().externalCatalog();
//        final Configuration configuration = session.sparkContext().hadoopConfiguration();
//        final List<CatalogTablePartition> partitions = deltaPartitionsBetween(configuration, afterSequence, beforeSequence);
//        final SortedMap<String, Map<UpsertOp, Set<Map<String, String>>>> entries = new TreeMap<>();
//        partitions.forEach(partition -> {
//            final scala.collection.Map<String, String> spec = partition.spec();
//            final String sequence = spec.get(SEQUENCE).get();
//            final UpsertOp operation = UpsertOp.valueOf(spec.get(OPERATION).get());
//            final Map<String, String> values = basePartition.stream().collect(Collectors.toMap(
//                    e -> e,
//                    e -> spec.get(e).get()
//            ));
//            entries.computeIfAbsent(sequence, ignored -> new HashMap<>())
//                    .computeIfAbsent(operation, ignored -> new HashSet<>())
//                    .add(values);
//        });
//        return entries.entrySet().stream()
//                .map(e -> new SequenceEntry(e.getKey(), e.getValue()))
//                .collect(Collectors.toList());
//    }
//
//    public SequenceEntry getLatestSequence(final SparkSession session) {
//
//        final List<SequenceEntry> sequence = getSequence(session);
//        if(sequence.isEmpty()) {
//            return null;
//        } else {
//            return sequence.get(sequence.size() - 1);
//        }
//    }

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

    private List<CatalogTablePartition> deltaPartitionsBetween(final Configuration configuration, final String afterSequence, final String beforeSequence) {

        return SparkCatalogUtils.findPartitions(configuration, deltaLocation, deltaPartition, new SparkCatalogUtils.FindPartitionsStrategy() {
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

    public Map<String, Map<UpsertOp, Set<Map<String, String>>>> rawSequenceBetween(final SparkSession session, final String afterSequence, final String beforeSequence) {

        final Configuration configuration = session.sparkContext().hadoopConfiguration();

        final Map<String, Map<UpsertOp, Set<Map<String, String>>>> sequences = new TreeMap<>();
        deltaPartitionsBetween(configuration, afterSequence, beforeSequence).forEach(part -> {
            final String sequence = part.spec().get(SEQUENCE).get();
            final UpsertOp op = UpsertOp.valueOf(part.spec().get(OPERATION).get());
            final Map<String, String> values = basePartition.stream().collect(Collectors.toMap(k -> k, k -> part.spec().get(k).get()));
            sequences.computeIfAbsent(sequence, ignored -> new HashMap<>())
                    .computeIfAbsent(op, ignored -> new HashSet<>())
                    .add(values);
        });
        return sequences;
    }

    public List<DeltaState.Sequence> sequence(final SparkSession session) {

        return sequenceBetween(session, minSequence(), maxSequence());
    }

    public List<DeltaState.Sequence> sequenceAfter(final SparkSession session, final String afterSequence) {

        return sequenceBetween(session, afterSequence, maxSequence());
    }

    public List<DeltaState.Sequence> sequenceBetween(final SparkSession session, final String afterSequence, final String beforeSequence) {

        final Map<String, Map<UpsertOp, Set<Map<String, String>>>> sequences = rawSequenceBetween(session, afterSequence, beforeSequence);
        return sequences.entrySet().stream().map(entry -> new DeltaState.Sequence(entry.getKey(), entry.getValue())).collect(Collectors.toList());
    }

    public DeltaState.Sequence latestSequence(final SparkSession session) {

        final List<DeltaState.Sequence> sequences = sequence(session);
        if(sequences.isEmpty()) {
            return null;
        } else {
            return sequences.get(sequences.size() - 1);
        }
    }

    public void replayDeltas(final SparkSession session, final String afterSequence, final String beforeSequence) {

        autoProvision(session);

        final Map<String, Map<UpsertOp, Set<Map<String, String>>>> sequences = rawSequenceBetween(session, afterSequence, beforeSequence);
        updateDeltaState(session, current -> current.mergeSequences(sequences));
    }

    public Source<Dataset<Row>> source(final SparkSession session) {

        return sink -> sink.accept(select(session, false));
    }

    public Query<Row> query(final SparkSession session) {

        return () -> select(session, false);
    }

    public Query<Row> queryDelta(final SparkSession session) {

        return () -> selectLatestDelta(session);
    }

    public Query<Row> queryBase(final SparkSession session) {

        return () -> selectBase(session, false);
    }

    public void dropBase(final SparkSession session, final boolean purge) {

        final ExternalCatalog catalog = session.sharedState().externalCatalog();
        if(tableName != null) {
            final String database = tableName.first();
            final String table = tableName.withoutFirst().toString("_");
            catalog.dropTable(database, table, true, purge);
        }
        state.dropState(session, BaseState.KEY);
        if(purge) {
            purge(session, baseLocation);
        }
    }

    public void dropDeltas(final SparkSession session, final boolean purge) {

        state.dropState(session, DeltaState.KEY);
        if(purge) {
            purge(session, deltaLocation);
        }
    }

    public UpsertState state() {

        return state;
    }

    public BaseState baseState(final SparkSession session) {

        autoProvision(session);

        return state.getState(session, BaseState.KEY, BaseState.class);
    }

    private BaseState updateBaseState(final SparkSession session, final Function<BaseState, BaseState> apply) {

        return state.updateState(session, BaseState.KEY, BaseState.class, apply);
    }

    public DeltaState deltaState(final SparkSession session) {

        autoProvision(session);

        return state.getState(session, DeltaState.KEY, DeltaState.class);
    }

    private DeltaState updateDeltaState(final SparkSession session, final Function<DeltaState, DeltaState> apply) {

        return state.updateState(session, DeltaState.KEY, DeltaState.class, apply);
    }

//    public Object getState(final SparkSession session, final String key) {
//
//        try {
//            final Configuration configuration = session.sparkContext().hadoopConfiguration();
//            final Path path = new Path(stateLocation());
//            final FileSystem fs = path.getFileSystem(configuration);
//            if(fs.exists(path)) {
//                try(final InputStream is = fs.open(path)) {
//                    final Map<?, ?> state = OBJECT_MAPPER.readValue(is, Map.class);
//                    return state.get(key);
//                }
//            } else {
//                return null;
//            }
//        } catch (final IOException e) {
//            throw new IllegalStateException("Failed to read state", e);
//        }
//    }
//
//    @SuppressWarnings("unchecked")
//    public void setState(final SparkSession session, final String key, final Object value) {
//
//        try {
//            final Configuration configuration = session.sparkContext().hadoopConfiguration();
//            final Path path = new Path(stateLocation());
//            final FileSystem fs = path.getFileSystem(configuration);
//            final Map<Object,  Object> state = new HashMap<>();
//            if(fs.exists(path)) {
//                try (final InputStream is = fs.open(path)) {
//                    OBJECT_MAPPER.readValue(is, Map.class).forEach(state::put);
//                }
//            }
//            state.put(key, value);
//            try(final OutputStream os = fs.create(path, true)) {
//                OBJECT_MAPPER.writeValue(os, state);
//            }
//        } catch (final IOException e) {
//            throw new IllegalStateException("Failed to read state", e);
//        }
//    }

    private Long getSizeInBytes(final SparkSession session, final Stream<URI> locations) {

        final Configuration configuration = session.sparkContext().hadoopConfiguration();
        return locations.mapToLong(location -> {
            try {
                final Path path = new Path(location.toString());
                final FileSystem fs = path.getFileSystem(configuration);
                long size = 0;
                if(fs.exists(path)) {
                    final RemoteIterator<LocatedFileStatus> iter = fs.listFiles(path, true);
                    while (iter.hasNext()) {
                        final LocatedFileStatus state = iter.next();
                        size += state.getBlockSize();
                    }
                }
                return size;
            } catch (final IOException e) {
                throw new IllegalStateException(e);
            }
        }).sum();
    }

    public long getBaseSizeInBytes(final SparkSession session) {

        final BaseState baseState = baseState(session);
        return getSizeInBytes(session, baseState.locations(baseLocation, basePartition, partitionFilter));
    }

    public long getDeltaSizeInBytes(final SparkSession session) {

        final DeltaState deltaState = deltaState(session);
        return getSizeInBytes(session, deltaState.locations(deltaLocation, basePartition, partitionFilter));
    }

    public long getSizeInBytes(final SparkSession session) {

        return getBaseSizeInBytes(session) + getDeltaSizeInBytes(session);
    }

    private static void purge(final SparkSession session, final URI location) {

        final Configuration configuration = session.sparkContext().hadoopConfiguration();
        try {
            final Path path = new Path(location);
            final FileSystem fs = path.getFileSystem(configuration);
            fs.delete(path, true);
        } catch (final IOException e) {
            log.error("Failed to purge " + location, e);
        }
    }

    private void autoProvision(final SparkSession session) {

        synchronized (this) {
            if(!provisioned) {
                provision(session);
                provisioned = true;
            }
        }
    }
}
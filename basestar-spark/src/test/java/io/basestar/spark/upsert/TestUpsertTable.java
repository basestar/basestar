package io.basestar.spark.upsert;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.basestar.mapper.annotation.Property;
import io.basestar.schema.Bucketing;
import io.basestar.schema.ReferableSchema;
import io.basestar.spark.AbstractSparkTest;
import io.basestar.spark.exception.DataIntegrityException;
import io.basestar.spark.transform.BucketTransform;
import io.basestar.spark.util.SparkCatalogUtils;
import io.basestar.spark.util.SparkRowUtils;
import io.basestar.util.Name;
import io.basestar.util.Sort;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.catalog.ExternalCatalog;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.*;

@Slf4j
class TestUpsertTable extends AbstractSparkTest {

    private Instant timestamp = Instant.ofEpochSecond(0);

    private String sequence() {

        timestamp = timestamp.plus(1, ChronoUnit.SECONDS);
        return UpsertTable.sequence(timestamp);
    }

    @Test
    void testUpsertChanges() {

        final SparkSession session = session();

        final Bucketing bucketing = new Bucketing(ImmutableList.of(Name.of(ReferableSchema.ID)), 2);
        final BucketTransform bucket = BucketTransform.builder()
                .bucketing(bucketing)
                .build();

        final String database = "tmp_" + UUID.randomUUID().toString().replaceAll("-", "_");
        final String location = testDataPath("spark/" + database);

        final ExternalCatalog catalog = session.sharedState().externalCatalog();

        SparkCatalogUtils.ensureDatabase(catalog, database, location);

        final List<D> create = ImmutableList.of(new D("d:1", 5L), new D("d:2", 4L), new D("d:3", 3L), new D("d:4", 2L));
        final List<Delta> createDeltas = ImmutableList.of(Delta.create(create.get(0)), Delta.create(create.get(1)),
                Delta.create(create.get(2)), Delta.create(create.get(3)));

        assertEquals(1, bucketing.apply(v -> "d:1"));
        assertEquals(0, bucketing.apply(v -> "d:2"));
        assertEquals(0, bucketing.apply(v -> "d:3"));
        assertEquals(0, bucketing.apply(v -> "d:4"));

        final Dataset<Row> createSource = bucket.accept(session.createDataset(create, Encoders.bean(D.class)).toDF());
        final StructType structType = createSource.schema();

        final UpsertTable table = UpsertTable.builder()
                .tableName(Name.of(database, "D"))
                .schema(structType)
                .baseLocation(URI.create(location + "/D/base"))
                .deltaLocation(URI.create(location + "/D/delta"))
                .state(new UpsertState.Hdfs(URI.create(location + "/D/state")))
                .partition(ImmutableList.of(bucket.getOutputColumn()))
                .idColumn(ReferableSchema.ID)
//                .minimizePartitions(true)
                .build();

        table.provision(session);

        table.applyChanges(createSource, sequence(), r -> UpsertOp.CREATE, r -> r);
        assertState("After create", session, table, ImmutableList.of(), createDeltas, create);

        table.squashDeltas(session);
        assertState("After create + flatten", session, table, create, ImmutableList.of(), create);

        final UpsertTable filtered0 = table.withPartitionFilter(ImmutableSet.of(ImmutableMap.of("__bucket", "0")));
        final List<D> bucket0 = ImmutableList.of(create.get(1), create.get(2), create.get(3));
        assertState("After create (filtered bucket 0)", session, filtered0, bucket0, ImmutableList.of(), bucket0);
        final List<D> bucket1 = ImmutableList.of(create.get(0));
        final UpsertTable filtered1 = table.withPartitionFilter(ImmutableSet.of(ImmutableMap.of("__bucket", "1")));
        assertState("After create (filtered bucket 1)", session, filtered1, bucket1, ImmutableList.of(), bucket1);

        final List<D> update = ImmutableList.of(new D("d:1", 2L), new D("d:3", 4L));
        final List<Delta> updateDeltas = ImmutableList.of(Delta.update(update.get(0)), Delta.update(update.get(1)));
        final List<D> updateMerged = ImmutableList.of(update.get(0), create.get(1), update.get(1), create.get(3));

        final Dataset<Row> updateSource = bucket.accept(session.createDataset(update, Encoders.bean(D.class)).toDF());

        table.applyChanges(updateSource, sequence(), r -> UpsertOp.UPDATE, r -> r);
        assertState("After update", session, table, create, updateDeltas, updateMerged);

        table.squashDeltas(session);
        assertState("After update + flatten", session, table, updateMerged, ImmutableList.of(), updateMerged);

        final List<D> delete = ImmutableList.of(new D("d:2", 3L), new D("d:4", 5L));
        final List<Delta> deleteDeltas = ImmutableList.of(Delta.delete(delete.get(0)), Delta.delete(delete.get(1)));
        final List<D> deleteMerged = ImmutableList.of(updateMerged.get(0), updateMerged.get(2));

        final Dataset<Row> deleteSource = bucket.accept(session.createDataset(delete, Encoders.bean(D.class)).toDF());
        table.applyChanges(deleteSource, sequence(), r -> UpsertOp.DELETE, r -> r);
        assertState("After delete", session, table, updateMerged, deleteDeltas, deleteMerged);

        table.squashDeltas(session);
        assertState("After delete + flatten", session, table, deleteMerged, ImmutableList.of(), deleteMerged);

        final List<DeltaState.Sequence> sequence = table.deltaState(session).getSequences();
        System.err.println(sequence);

        final Dataset<Row> deletes = table.deletesBetween(session, UpsertTable.minSequence(), UpsertTable.maxSequence()).orElseThrow(IllegalStateException::new);
        assertEquals(2, deletes.count());

        table.dropBase(session, true);
        table.dropDeltas(session, false);
        table.repair(session);

        assertState("After purge", session, table, ImmutableList.of(), ImmutableList.of(), ImmutableList.of());

        table.replayDeltas(session, UpsertTable.minSequence(), UpsertTable.maxSequence());

        log.warn("Raw delta sequence is: {}", table.selectDelta(session).orderBy(UpsertTable.SEQUENCE).collectAsList());

        table.squashDeltas(session);

        assertState("After replay", session, table, deleteMerged, ImmutableList.of(), deleteMerged);

        // Check that merging works correctly (i.e. appending to existing partitions)

        final List<D> merge = ImmutableList.of(new D("d:5", 5L), new D("d:6", 4L));
        final List<Delta> mergeDeltas = ImmutableList.of(Delta.create(merge.get(0)), Delta.create(merge.get(1)));
        final Dataset<Row> mergeSource = bucket.accept(session.createDataset(merge, Encoders.bean(D.class)).toDF());

        final List<D> result = ImmutableList.<D>builder().addAll(deleteMerged).addAll(merge).build();

        table.applyChanges(mergeSource, sequence(), r -> UpsertOp.CREATE, r -> r);
        assertState("After merge", session, table, deleteMerged, mergeDeltas, result);

        table.squashDeltas(session);
        assertState("After merge + flatten", session, table, result, ImmutableList.of(), result);

        final String database2 = database + "_copy";
        final String location2 = testDataPath("spark/" + database2);

        SparkCatalogUtils.ensureDatabase(catalog, database2, location2 + "/D");

        final UpsertTable table2 = table.copy(session, Name.of(database2, "D"), URI.create(location2 + "/D/base"), URI.create(location2 + "/D/delta"), new UpsertState.Hdfs(URI.create(location2 + "/D/state")));
        assertState("After copy", session, table2, result, ImmutableList.of(), result);

        final List<D> create2 = ImmutableList.of(new D("d:8", 5L));
        final Dataset<Row> createSource2 = bucket.accept(session.createDataset(create2, Encoders.bean(D.class)).toDF());

        table.applyChanges(createSource2, sequence(), r -> UpsertOp.CREATE, r -> r);
        log.warn("Result: {}", table.select(session).collectAsList());
    }

    private void assertState(final String step, final SparkSession session, final UpsertTable table,
                             final List<D> expectBase, final List<Delta> expectDelta, final List<D> expectMerged) {

        final List<D> actualBase = table.queryBase(session).sort(Sort.asc(ReferableSchema.ID)).as(D.class).collectAsList();
        final List<Delta> actualDelta = table.queryDelta(session).sort(Sort.asc(ReferableSchema.ID)).as(Delta.class).collectAsList();
        final List<D> actualMerged = table.query(session).sort(Sort.asc(ReferableSchema.ID)).as(D.class).collectAsList();

        log.warn(step + " base: " + actualBase);
        assertEquals(expectBase, actualBase);

        log.warn(step + " delta: " + actualDelta);
        assertEquals(expectDelta, actualDelta);

        log.warn(step + " merged: " + actualMerged);
        assertEquals(expectMerged, actualMerged);
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Delta {

        @Property(name = "__operation")
        private String operation;

        @Property(name = "id")
        private String id;

        @Property(name = "x")
        private Long x;

        public static Delta create(final D after) {

            return new Delta(UpsertOp.CREATE.name(), after.getId(), after.getX());
        }

        public static Delta update(final D after) {

            return new Delta(UpsertOp.UPDATE.name(), after.getId(), after.getX());
        }

        public static Delta delete(final D before) {

            return new Delta(UpsertOp.DELETE.name(), before.getId(), before.getX());
        }
    }

    @Test
    void testAlterSchema() {

        final SparkSession session = session();

        final String database = "tmp_" + UUID.randomUUID().toString().replaceAll("-", "_");
        final String location = testDataPath("spark/" + database);

        final ExternalCatalog catalog = session.sharedState().externalCatalog();
        SparkCatalogUtils.ensureDatabase(catalog, database, location);

        final List<D> create = ImmutableList.of(new D("d:1", 5L), new D("d:2", 4L), new D("d:3", 3L), new D("d:4", 2L));

        final BucketTransform bucket = BucketTransform.builder()
                .bucketing(new Bucketing(ImmutableList.of(Name.of(ReferableSchema.ID)), 2))
                .build();

        final Dataset<Row> createSource = bucket.accept(session.createDataset(create, Encoders.bean(D.class)).toDF());
        final StructType initial = createSource.schema();

        final UpsertTable table = UpsertTable.builder()
                .tableName(Name.of(database, "D"))
                .schema(initial)
                .baseLocation(URI.create(location + "/D/base"))
                .deltaLocation(URI.create(location + "/D/delta"))
                .state(new UpsertState.Hdfs(URI.create(location + "/D/state")))
                .partition(ImmutableList.of("__bucket"))
                .idColumn(ReferableSchema.ID)
                .build();
        table.provision(session);

        final StructType appended = SparkRowUtils.append(initial, SparkRowUtils.field("test", DataTypes.BinaryType));

        final UpsertTable table2 = table.withSchema(appended);
        table2.provision(session);
    }

    @Test
    void testValidateAndDeduplicate() {

        final SparkSession session = session();

        final String database = "tmp_" + UUID.randomUUID().toString().replaceAll("-", "_");
        final String location = testDataPath("spark/" + database);

        final ExternalCatalog catalog = session.sharedState().externalCatalog();
        SparkCatalogUtils.ensureDatabase(catalog, database, location);

        final List<D> create = ImmutableList.of(new D("d:1", 5L), new D("d:2", 4L), new D("d:3", 3L), new D("d:4", 2L));

        final BucketTransform bucket = BucketTransform.builder()
                .bucketing(new Bucketing(ImmutableList.of(Name.of(ReferableSchema.ID)), 2))
                .build();

        final Dataset<Row> createSource = bucket.accept(session.createDataset(create, Encoders.bean(D.class)).toDF());
        final StructType initial = createSource.schema();

        final UpsertTable table = UpsertTable.builder()
                .tableName(Name.of(database, "D"))
                .schema(initial)
                .baseLocation(URI.create(location + "/D/base"))
                .deltaLocation(URI.create(location + "/D/delta"))
                .state(new UpsertState.Hdfs(URI.create(location + "/D/state")))
                .partition(ImmutableList.of("__bucket"))
                .idColumn(ReferableSchema.ID)
                .build();
        table.provision(session);

        table.applyChanges(createSource, sequence(), r -> UpsertOp.CREATE, r -> r);
        table.squashDeltas(session);

        final long originalCount = table.select(session).count();

        table.applyChanges(createSource, sequence(), r -> UpsertOp.CREATE, r -> r);

        assertThrows(DataIntegrityException.class, () -> table.validate(session));

        table.squashDeltas(session);

        assertThrows(DataIntegrityException.class, () -> table.validate(session));

        table.forceDeduplicate(session, sequence());

        final long count = table.select(session).count();

        assertEquals(originalCount, count);

        table.validate(session);
    }

    @Test
    void testCopy() {

        final SparkSession session = session();

        final String database = "tmp_" + UUID.randomUUID().toString().replaceAll("-", "_");
        final String location = testDataPath("spark/" + database);

        final ExternalCatalog catalog = session.sharedState().externalCatalog();
        SparkCatalogUtils.ensureDatabase(catalog, database, location);

        final List<D> create = ImmutableList.of(new D("d:1", 5L), new D("d:2", 4L), new D("d:3", 3L), new D("d:4", 2L));

        final BucketTransform bucket = BucketTransform.builder()
                .bucketing(new Bucketing(ImmutableList.of(Name.of(ReferableSchema.ID)), 2))
                .build();

        final Dataset<Row> createSource = bucket.accept(session.createDataset(create, Encoders.bean(D.class)).toDF());
        final StructType initial = createSource.schema();

        final UpsertTable table = UpsertTable.builder()
                .tableName(null)
                .schema(initial)
                .baseLocation(URI.create(location + "/D/base"))
                .deltaLocation(URI.create(location + "/D/delta"))
                .state(new UpsertState.Hdfs(URI.create(location + "/D/state")))
                .partition(ImmutableList.of("__bucket"))
                .idColumn(ReferableSchema.ID)
                .build();

        table.provision(session);

        table.applyChanges(createSource, sequence(), r -> UpsertOp.CREATE, r -> r);
        table.squashDeltas(session);

        final long originalCount = table.select(session).count();
        assertThat(originalCount).isEqualTo(4);

        assertAll(
                () -> assertCopyToNonEmptyBaseThrows(session, location, table),
                () -> assertCopyToNonEmptyDeltaThrows(session, location, table),
                () -> assertCopyToEmptyBaseDeltaCopies(session, location, table, originalCount),
                () -> assertCopyToNewBaseDeltaCopies(session, location, table, originalCount)
        );
    }

    private void assertCopyToNonEmptyBaseThrows(final SparkSession session, final String location, final UpsertTable table) {
        // create non-empty base dir and check that copy is not allowed
        final URI nonEmptyBase = URI.create(location + "/NEB/base");
        final URI nonEmptyBaseDelta = URI.create(location + "/NEB/delta");

        Paths.get(nonEmptyBase).resolve("anotherSubDir").toFile().mkdirs();
        Paths.get(nonEmptyBaseDelta).toFile().mkdirs();

        assertThatThrownBy(() -> table.copy(session, null, nonEmptyBase, nonEmptyBaseDelta, table.state))
                .hasMessageContaining("Cannot copy table - output location is not empty")
                .hasMessageContaining(nonEmptyBase.toString());
    }

    private void assertCopyToNonEmptyDeltaThrows(final SparkSession session, final String location, final UpsertTable table) {
        // create non-empty delta dir and check that copy is not allowed
        final URI nonEmptyDeltaBase = URI.create(location + "/NED/base");
        final URI nonEmptyDelta = URI.create(location + "/NED/delta");

        Paths.get(nonEmptyDeltaBase).toFile().mkdirs();
        Paths.get(nonEmptyDelta).resolve("anotherSubDir").toFile().mkdirs();

        assertThatThrownBy(() -> table.copy(session, null, nonEmptyDeltaBase, nonEmptyDelta, table.state))
                .hasMessageContaining("Cannot copy table - output location is not empty")
                .hasMessageContaining(nonEmptyDelta.toString());
    }

    private void assertCopyToEmptyBaseDeltaCopies(final SparkSession session, final String location, final UpsertTable table, final long originalCount) {
        // create empty base/delta dir and check that copy is successful
        final URI emptyBase = URI.create(location + "/E/base");
        final URI emptyDelta = URI.create(location + "/E/delta");

        Paths.get(emptyBase).toFile().mkdirs();
        Paths.get(emptyDelta).toFile().mkdirs();

        table.copy(session, null, emptyBase, emptyDelta, table.state);
        assertThat(table.select(session).count()).isEqualTo(originalCount);
    }

    private void assertCopyToNewBaseDeltaCopies(final SparkSession session, final String location, final UpsertTable table, final long originalCount) {
        // provide non-existent paths delta/base paths and check that copy is successful
        final URI newBase = URI.create(location + "/N/base");
        final URI newDelta = URI.create(location + "/N/delta");

        assertThat(Paths.get(newBase).toFile()).doesNotExist();
        assertThat(Paths.get(newDelta).toFile()).doesNotExist();

        table.copy(session, null, newBase, newDelta, table.state);
        assertThat(table.select(session).count()).isEqualTo(originalCount);
    }

}

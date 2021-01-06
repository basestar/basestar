package io.basestar.spark.upsert;

import com.google.common.collect.ImmutableList;
import io.basestar.mapper.annotation.Property;
import io.basestar.schema.Bucketing;
import io.basestar.schema.ReferableSchema;
import io.basestar.spark.AbstractSparkTest;
import io.basestar.spark.transform.BucketTransform;
import io.basestar.spark.util.SparkCatalogUtils;
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
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
class TestUpsertTable extends AbstractSparkTest {

    @Test
    void testUpsertChanges() {

        final SparkSession session = session();

        final BucketTransform bucket = BucketTransform.builder()
                .bucketing(new Bucketing(Name.of(ReferableSchema.ID)))
                .build();

        final String database = "tmp_" + UUID.randomUUID().toString().replaceAll("-", "_");
        final String location = testDataPath("spark/" + database);

        final ExternalCatalog catalog = session.sharedState().externalCatalog();

        SparkCatalogUtils.ensureDatabase(catalog, database, location);

        final List<D> create = ImmutableList.of(new D("d:1", 5L), new D("d:2", 4L), new D("d:3", 3L), new D("d:4", 2L));
        final List<Delta> createDeltas = ImmutableList.of(Delta.create(create.get(0)), Delta.create(create.get(1)),
                Delta.create(create.get(2)), Delta.create(create.get(3)));

        final Dataset<Row> createSource = bucket.accept(session.createDataset(create, Encoders.bean(D.class)).toDF());
        final StructType structType = createSource.schema();

        final UpsertTable table = UpsertTable.builder()
                .database(database)
                .name("D")
                .structType(structType)
                .location(location + "/D")
                .partition(ImmutableList.of(bucket.getOutputColumn()))
                .idColumn(ReferableSchema.ID)
                .build();

        table.provision(session);

        table.applyChanges(createSource, UpsertTable.sequence(Instant.now()), r -> UpsertOp.CREATE, r -> r);
        assertState("After create", session, table, ImmutableList.of(), createDeltas, create);

        table.squashDeltas(session);
        assertState("After create + flatten", session, table, create, ImmutableList.of(), create);

        final List<D> update = ImmutableList.of(new D("d:1", 2L), new D("d:3", 4L));
        final List<Delta> updateDeltas = ImmutableList.of(Delta.update(update.get(0)), Delta.update(update.get(1)));
        final List<D> updateMerged = ImmutableList.of(update.get(0), create.get(1), update.get(1), create.get(3));

        final Dataset<Row> updateSource = bucket.accept(session.createDataset(update, Encoders.bean(D.class)).toDF());

        table.applyChanges(updateSource, UpsertTable.sequence(Instant.now()), r -> UpsertOp.UPDATE, r -> r);
        assertState("After update", session, table, create, updateDeltas, updateMerged);

        table.squashDeltas(session);
        assertState("After update + flatten", session, table, updateMerged, ImmutableList.of(), updateMerged);

//        // FIXME: failing periodically because of a possible list-after-write inconsistency
//        final List<D> delete = ImmutableList.of(new D("d:2", 3L), new D("d:4", 5L));
//        final List<Delta> deleteDeltas = ImmutableList.of(Delta.delete(delete.get(0)), Delta.delete(delete.get(1)));
//        final List<D> deleteMerged = ImmutableList.of(updateMerged.get(0), updateMerged.get(2));
//
//        final Dataset<Row> deleteSource = bucket.accept(session.createDataset(delete, Encoders.bean(D.class)).toDF());
//        table.applyChanges(deleteSource, UpsertTable.sequence(Instant.now()), r -> UpsertOp.DELETE, r -> r);
//        assertState("After delete", session, table, updateMerged, deleteDeltas, deleteMerged);
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
}

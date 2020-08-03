package io.basestar.spark;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.basestar.schema.Namespace;
import io.basestar.spark.database.SparkDatabase;
import io.basestar.spark.util.ColumnResolver;
import io.basestar.spark.util.DatasetResolver;
import io.basestar.util.Name;
import io.basestar.util.Sort;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SparkDatabaseTest extends AbstractSparkTest {

    @Test
    public void testDatabase() throws IOException {

        final SparkSession session = SparkSession.builder()
                .master("local[*]")
                .getOrCreate();

        final Namespace namespace = Namespace.load(SparkDatabaseTest.class.getResourceAsStream("schema.yml"));

        final Dataset<Row> datasetA = session.createDataset(ImmutableList.of(
                new A("a:1", new B("b:1"), ImmutableList.of(new B("b:3"), new B("b:4")), ImmutableMap.of("x", new B("b:5")), new E(new B("b:1"))),
                new A("a:2", new B("b:2"), ImmutableList.of(), ImmutableMap.of(), null)
        ), Encoders.bean(A.class)).toDF();

        final Map<Name, Dataset<Row>> datasets = ImmutableMap.of(
                Name.of("A"), datasetA
        );

        final DatasetResolver resolver = DatasetResolver.automatic((schema) -> datasets.get(schema.getQualifiedName()));

        final SparkDatabase database = SparkDatabase.builder()
                .resolver(resolver).namespace(namespace)
                .columnResolver(ColumnResolver.lowercase((ds, name) -> ColumnResolver.<Row>nested(ds, name)))
                .build();

        final List<A> results = database.from("A")
                .sort(Sort.desc("ref.id")).as(A.class)
                .query().collectAsList();

        assertEquals(2, results.size());
    }

//    @Test
//    public void testDateTime() throws IOException {
//
//        final SparkSession session = SparkSession.builder()
//                .master("local[*]")
//                .getOrCreate();
//
//        final Namespace namespace = Namespace.load(TestExpandTransform.class.getResourceAsStream("schema.yml"));
//
//        final LocalDate date = LocalDate.now();
//        final LocalDateTime datetime = LocalDateTime.now();
//
//        final java.sql.Date sparkDate = new java.sql.Date(date.atStartOfDay().toEpochSecond(ZoneOffset.UTC) * 1000);
//        final java.sql.Timestamp sparkDateTime = new java.sql.Timestamp(datetime.toEpochSecond(ZoneOffset.UTC) * 1000);
//
//        final Dataset<Row> datasetF = session.createDataset(ImmutableList.of(
//                new F(sparkDate, sparkDateTime)
//        ), Encoders.bean(F.class)).toDF();
//
//        final Map<Name, Dataset<Row>> datasets = ImmutableMap.of(
//                Name.of("F"), datasetF
//        );
//
//        final DatasetResolver resolver = DatasetResolver.automatic((schema) -> datasets.get(schema.getQualifiedName()));
//
//        final SparkDatabase database = SparkDatabase.builder()
//                .resolver(resolver).namespace(namespace)
//                .build();
//
//        final List<F2> results = database.from("F").as(F2.class).query().collectAsList();
//
//        assertEquals(1, results.size());
//        assertTrue(results.contains(new F2(date, datetime)));
//    }
}

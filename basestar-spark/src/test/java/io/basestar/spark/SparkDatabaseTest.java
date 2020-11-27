package io.basestar.spark;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.basestar.schema.Namespace;
import io.basestar.schema.ObjectSchema;
import io.basestar.spark.combiner.Combiner;
import io.basestar.spark.database.SparkDatabase;
import io.basestar.spark.resolver.SchemaResolver;
import io.basestar.spark.sink.MultiSink;
import io.basestar.spark.sink.Sink;
import io.basestar.spark.source.JoinedSource;
import io.basestar.spark.transform.CombinerTransform;
import io.basestar.spark.transform.IndexDeltaTransform;
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
import java.util.Set;

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

        final SchemaResolver resolver = new SchemaResolver.Automatic((schema, expand) -> datasets.get(schema.getQualifiedName()));

        final SparkDatabase database = SparkDatabase.builder()
                .resolver(resolver).namespace(namespace)
                .build();

        final List<A> results = database.from("A")
                .sort(Sort.desc("ref.id")).as(A.class)
                .query().collectAsList();

        assertEquals(2, results.size());
    }

    @Test
    public void testOverlay() throws IOException {

        final SparkSession session = SparkSession.builder()
                .master("local[*]")
                .getOrCreate();

        final Namespace namespace = Namespace.load(SparkDatabaseTest.class.getResourceAsStream("schema.yml"));

        final A a1 = new A("a:1", new B("b:1"), ImmutableList.of(new B("b:3"), new B("b:4")), ImmutableMap.of("x", new B("b:5")), new E(new B("b:1")));
        final A a2a = new A("a:2", new B("b:2"), ImmutableList.of(), ImmutableMap.of(), null);
        final A a2b = new A("a:2", new B("b:3"), ImmutableList.of(), ImmutableMap.of(), null);
        final A a3 = new A("a:3", new B("b:2"), ImmutableList.of(), ImmutableMap.of(), null);

        final Map<Name, Dataset<Row>> datasets1 = ImmutableMap.of(
                Name.of("A"), session.createDataset(ImmutableList.of(a1, a2a), Encoders.bean(A.class)).toDF()
        );

        final Map<Name, Dataset<Row>> datasets2 = ImmutableMap.of(
                Name.of("A"), session.createDataset(ImmutableList.of(a2b, a3), Encoders.bean(A.class)).toDF()
        );

        final SchemaResolver baseline = (schema, expand) -> datasets1.get(schema.getQualifiedName());
        final SchemaResolver overlay = (schema, expand) -> datasets2.get(schema.getQualifiedName());

        final SchemaResolver combinedResolver = new SchemaResolver.Automatic(new SchemaResolver.Combining(baseline, overlay, Combiner.Consistent.builder().build()));

        final SparkDatabase combinedDatabase = SparkDatabase.builder()
                .resolver(combinedResolver).namespace(namespace)
                .build();

        final Set<A> combinedA = ImmutableSet.copyOf(combinedDatabase.from("A").as(A.class).query().collectAsList());
        assertEquals(ImmutableSet.of(a1, a2b, a3), combinedA);

        final Set<StatsA> combinedStatsA = ImmutableSet.copyOf(combinedDatabase.from("StatsA").as(StatsA.class).query().collectAsList());
        assertEquals(ImmutableSet.of(
                new StatsA("b:1", 1L),
                new StatsA("b:2", 1L),
                new StatsA("b:3", 1L)
        ), combinedStatsA);

        final SchemaResolver baselineResolver = new SchemaResolver.Automatic(baseline);

        final SchemaResolver deltaResolver = new SchemaResolver.Combining(baselineResolver, combinedResolver, Combiner.DELTA);

        final SparkDatabase deltaDatabase = SparkDatabase.builder()
                .resolver(deltaResolver).namespace(namespace)
                .build();

        final Set<A> deltaA = ImmutableSet.copyOf(deltaDatabase.from("A").as(A.class).query().collectAsList());
        assertEquals(ImmutableSet.of(a2b, a3), deltaA);

        final Set<StatsA> deltaStatsA = ImmutableSet.copyOf(deltaDatabase.from("StatsA").as(StatsA.class).query().collectAsList());
        assertEquals(ImmutableSet.of(
                new StatsA("b:3", 1L)
        ), deltaStatsA);

        final ObjectSchema schemaA = namespace.requireObjectSchema("A");

        final JoinedSource<Row> joined = JoinedSource.<Row>builder()
                .left(baselineResolver.source(schemaA))
                .right(combinedResolver.source(schemaA))
                .idColumns(ImmutableList.of(schemaA.id()))
                .build();

        final CombinerTransform combinerTransform = CombinerTransform.builder()
                .schema(schemaA)
                .combiner(Combiner.DELTA)
                .build();

        final IndexDeltaTransform indexDeltaTransform = IndexDeltaTransform.builder()
                .schema(schemaA)
                .index(schemaA.requireIndex("ref", true))
                .build();

        joined.then(new MultiSink<>(
                combinerTransform.then(new Sink<Dataset<Row>>() {
                    @Override
                    public void accept(final Dataset<Row> input) {
                        System.err.println(input.collectAsList());
                    }
                }),
                indexDeltaTransform.then(new Sink<Dataset<Row>>() {
                    @Override
                    public void accept(final Dataset<Row> input) {
                        System.err.println(input.collectAsList());
                    }
                })
        ));
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

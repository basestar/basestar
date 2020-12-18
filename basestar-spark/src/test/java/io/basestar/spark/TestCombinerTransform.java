package io.basestar.spark;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.basestar.mapper.MappingContext;
import io.basestar.schema.Namespace;
import io.basestar.schema.ObjectSchema;
import io.basestar.spark.combiner.Combiner;
import io.basestar.spark.database.SparkDatabase;
import io.basestar.spark.query.QueryResolver;
import io.basestar.spark.sink.MultiSink;
import io.basestar.spark.source.JoinedSource;
import io.basestar.spark.transform.CombinerTransform;
import io.basestar.spark.transform.IndexDeltaTransform;
import io.basestar.spark.transform.MarshallTransform;
import io.basestar.util.Name;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TestCombinerTransform extends AbstractSparkTest {

    @Data
    @ToString(callSuper = true)
    @EqualsAndHashCode(callSuper = true)
    @io.basestar.mapper.annotation.ObjectSchema(name = "A")
    public static class IndexA extends A {

        private byte[] __partition;

        private byte[] __sort;

        private boolean __deleted;
    }

    @Test
    void testCombiner() throws IOException {

        final SparkSession session = session();

        final Namespace namespace = Namespace.load(SparkDatabaseTest.class.getResourceAsStream("schema.yml"));

        final AbstractSparkTest.A a1 = new AbstractSparkTest.A("a:1", new AbstractSparkTest.B("b:1"), ImmutableList.of(new AbstractSparkTest.B("b:3"), new AbstractSparkTest.B("b:4")), ImmutableMap.of("x", new AbstractSparkTest.B("b:5")), new AbstractSparkTest.E(new AbstractSparkTest.B("b:1")));
        final AbstractSparkTest.A a2a = new AbstractSparkTest.A("a:2", new AbstractSparkTest.B("b:2"), ImmutableList.of(), ImmutableMap.of(), null);
        final AbstractSparkTest.A a2b = new AbstractSparkTest.A("a:2", new AbstractSparkTest.B("b:3"), ImmutableList.of(), ImmutableMap.of(), null);
        final AbstractSparkTest.A a3 = new AbstractSparkTest.A("a:3", new AbstractSparkTest.B("b:2"), ImmutableList.of(), ImmutableMap.of(), null);

        final Map<Name, Dataset<Row>> datasets1 = ImmutableMap.of(
                Name.of("A"), session.createDataset(ImmutableList.of(a1, a2a), Encoders.bean(AbstractSparkTest.A.class)).toDF()
        );

        final Map<Name, Dataset<Row>> datasets2 = ImmutableMap.of(
                Name.of("A"), session.createDataset(ImmutableList.of(a2b, a3), Encoders.bean(AbstractSparkTest.A.class)).toDF()
        );

        final QueryResolver baseline = QueryResolver.source(schema -> datasets1.get(schema.getQualifiedName()));
        final QueryResolver overlay = QueryResolver.source(schema -> datasets2.get(schema.getQualifiedName()));

        final QueryResolver combinedResolver = new QueryResolver.Automatic(new QueryResolver.Combining(baseline, overlay, Combiner.Consistent.builder().build()));

        final SparkDatabase combinedDatabase = SparkDatabase.builder()
                .resolver(combinedResolver).namespace(namespace)
                .build();

        final Set<AbstractSparkTest.A> combinedA = ImmutableSet.copyOf(combinedDatabase.from("A").as(AbstractSparkTest.A.class).query().collectAsList());
        assertEquals(ImmutableSet.of(a1, a2b, a3), combinedA);

        final Set<AbstractSparkTest.StatsA> combinedStatsA = ImmutableSet.copyOf(combinedDatabase.from("StatsA").as(AbstractSparkTest.StatsA.class).query().collectAsList());
        assertEquals(ImmutableSet.of(
                new AbstractSparkTest.StatsA("b:1", 1L),
                new AbstractSparkTest.StatsA("b:2", 1L),
                new AbstractSparkTest.StatsA("b:3", 1L)
        ), combinedStatsA);

        final QueryResolver baselineResolver = new QueryResolver.Automatic(baseline);

        final QueryResolver deltaResolver = new QueryResolver.Combining(baselineResolver, combinedResolver, Combiner.DELTA);

        final SparkDatabase deltaDatabase = SparkDatabase.builder()
                .resolver(deltaResolver).namespace(namespace)
                .build();

        final Set<AbstractSparkTest.A> deltaA = ImmutableSet.copyOf(deltaDatabase.from("A").as(AbstractSparkTest.A.class).query().collectAsList());
        assertEquals(ImmutableSet.of(a2b, a3), deltaA);

        final Set<AbstractSparkTest.StatsA> deltaStatsA = ImmutableSet.copyOf(deltaDatabase.from("StatsA").as(AbstractSparkTest.StatsA.class).query().collectAsList());
        assertEquals(ImmutableSet.of(
                new AbstractSparkTest.StatsA("b:3", 1L)
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

        final MarshallTransform<A> marshallA = MarshallTransform.<A>builder().context(new MappingContext()).targetType(A.class).build();

        joined.then(new MultiSink<>(
                combinerTransform.then(marshallA).then(input -> {

                    final Set<A> delta = ImmutableSet.copyOf(input.collectAsList());
                    assertEquals(ImmutableSet.of(a2b, a3), delta);
                }),
                indexDeltaTransform.then(input -> {

                    final Set<Row> delta = ImmutableSet.copyOf(input.collectAsList());
                    System.err.println(delta);
//                    assertEquals(ImmutableSet.of(a2b, a3), delta);
                })
        ));
    }
}

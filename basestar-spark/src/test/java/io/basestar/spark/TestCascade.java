package io.basestar.spark;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.basestar.expression.Expression;
import io.basestar.mapper.MappingContext;
import io.basestar.schema.Namespace;
import io.basestar.schema.ReferableSchema;
import io.basestar.schema.util.Cascade;
import io.basestar.spark.transform.MarshallTransform;
import io.basestar.spark.util.CascadeJoiner;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestCascade extends AbstractSparkTest {

    @Test
    public void testCascadeJoiner() throws IOException {

        final SparkSession session = session();
        final Namespace namespace = Namespace.load(SparkDatabaseTest.class.getResourceAsStream("schema.yml"));

        final ReferableSchema schemaA = namespace.requireReferableSchema("A");
        final Dataset<Row> datasetA = session.createDataset(ImmutableList.of(
                new A("a:1", null, ImmutableList.of(), ImmutableMap.of(), null)
        ), Encoders.bean(A.class)).toDF();

        final ReferableSchema schemaC = namespace.requireReferableSchema("C");
        final Dataset<Row> datasetC = session.createDataset(ImmutableList.of(
                new C("c:1", new A("a:1")),
                new C("c:2", new A("a:2")),
                new C("c:3", new A("a:1"))
        ), Encoders.bean(C.class)).toDF();

        final Set<Expression> queries = schemaC.cascadeQueries(Cascade.DELETE, schemaA.getQualifiedName());
        final CascadeJoiner cascadeJoiner = new CascadeJoiner(schemaA, schemaC, queries);

        final Dataset<Row> ds = cascadeJoiner.cascade(datasetA, datasetC);

        final MarshallTransform<C> marshall = MarshallTransform.<C>builder().context(new MappingContext()).targetType(C.class).build();
        final List<C> result = marshall.accept(ds.sort("id")).collectAsList();

        assertEquals(ImmutableList.of(
                new C("c:1", new A("a:1")),
                new C("c:3", new A("a:1"))
        ), result);
    }
}

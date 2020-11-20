package io.basestar.spark;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.basestar.schema.Namespace;
import io.basestar.schema.ObjectSchema;
import io.basestar.schema.layout.ObjectIdLayout;
import io.basestar.spark.resolver.SchemaResolver;
import io.basestar.spark.transform.LayoutTransform;
import io.basestar.util.Name;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class TestLayoutTransform extends AbstractSparkTest {

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class FlatA {

        private String id;

        private String ref;
    }

    @Test
    public void testObjectIdLayout() throws Exception {

        final SparkSession session = SparkSession.builder()
                .master("local[*]")
                .getOrCreate();

        final Namespace namespace = Namespace.load(SparkDatabaseTest.class.getResourceAsStream("schema.yml"));

        final ObjectSchema a = namespace.requireObjectSchema("A");

        final Dataset<Row> datasetA = session.createDataset(ImmutableList.of(
                new AbstractSparkTest.A("a:1", new AbstractSparkTest.B("b:1")),
                new AbstractSparkTest.A("a:2", new AbstractSparkTest.B("b:2"))
        ), Encoders.bean(AbstractSparkTest.A.class)).toDF();

        final Map<Name, Dataset<Row>> datasets = ImmutableMap.of(
                Name.of("A"), datasetA
        );

        final SchemaResolver resolver = new SchemaResolver.Automatic((schema) -> datasets.get(schema.getQualifiedName()));
//        final ColumnResolver<Row> columnResolver = ColumnResolver.lowercase(ColumnResolver::nested);

        final Dataset<Row> ds = resolver.resolve(a);

        final LayoutTransform layout = LayoutTransform.builder()
                .inputLayout(a)
                .outputLayout(new ObjectIdLayout(a))
                .build();

        System.err.println(layout.accept(ds).collectAsList());
    }
}

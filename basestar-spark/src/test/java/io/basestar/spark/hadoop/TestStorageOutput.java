package io.basestar.spark.hadoop;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.basestar.expression.Expression;
import io.basestar.schema.Consistency;
import io.basestar.schema.Instance;
import io.basestar.schema.Namespace;
import io.basestar.schema.ObjectSchema;
import io.basestar.schema.util.Ref;
import io.basestar.spark.AbstractSparkTest;
import io.basestar.spark.database.SparkDatabase;
import io.basestar.spark.query.QueryResolver;
import io.basestar.spark.util.SparkSchemaUtils;
import io.basestar.storage.MemoryStorage;
import io.basestar.storage.Storage;
import io.basestar.util.Name;
import io.basestar.util.Page;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.*;
import org.junit.jupiter.api.Test;
import scala.Tuple2;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class TestStorageOutput extends AbstractSparkTest {

    private static final MemoryStorage storage = MemoryStorage.builder().build();

    private static final Namespace namespace;
    static {
        try {
            namespace = Namespace.load(AbstractSparkTest.class.getResourceAsStream("schema.yml"));
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static class ExampleStorageProvider implements StorageProvider {

        @Override
        public Namespace namespace(final Configuration configuration) {

            return namespace;
        }

        @Override
        public Storage storage(final Configuration configuration) {

            return storage;
        }
    }

    @Test
    void testStorageOutput() {

        final SparkSession session = session();

        final Dataset<Row> datasetA = session.createDataset(ImmutableList.of(
                new AbstractSparkTest.A("a:1", new AbstractSparkTest.B("b:1"), ImmutableList.of(new AbstractSparkTest.B("b:3"), new AbstractSparkTest.B("b:4")), ImmutableMap.of("x", new AbstractSparkTest.B("b:5")), new AbstractSparkTest.E(new AbstractSparkTest.B("b:1"))),
                new AbstractSparkTest.A("a:2", new AbstractSparkTest.B("b:2"), ImmutableList.of(), ImmutableMap.of(), null)
        ), Encoders.bean(AbstractSparkTest.A.class)).toDF();

        final Map<Name, Dataset<Row>> datasets = ImmutableMap.of(
                Name.of("A"), datasetA
        );

        final ObjectSchema schemaA = namespace.requireObjectSchema("A");

        final QueryResolver resolver = new QueryResolver.Automatic(QueryResolver.ofSources(schema -> datasets.get(schema.getQualifiedName())));

        final SparkDatabase database = SparkDatabase.builder()
                .resolver(resolver).namespace(namespace)
                .build();

        final Configuration config = new Configuration();

        config.set(StorageProvider.PROVIDER, ExampleStorageProvider.class.getName());

        database.from("A").query().withColumn("version", functions.lit(1L)).toJavaRDD().mapToPair(row -> {
            final Map<String, Object> after = SparkSchemaUtils.fromSpark(schemaA, row);
            return Tuple2.apply(Ref.of(schemaA.getQualifiedName(), Instance.getId(after)), new WriteAction.Create(after));
        }).saveAsNewAPIHadoopFile("", Ref.class, WriteAction.class, StorageOutputFormat.class, config);

        final Map<String, Object> a1 = storage.get(Consistency.ATOMIC, schemaA, "a:1", ImmutableSet.of()).join();
        assertNotNull(a1);
        final Map<String, Object> a2 = storage.get(Consistency.ATOMIC, schemaA, "a:2", ImmutableSet.of()).join();
        assertNotNull(a2);

        // Check indexes were written
        final Page<Map<String, Object>> b1 = storage.query(schemaA, Expression.parse("ref.id == 'b:1'"), ImmutableList.of(), ImmutableSet.of())
                .page(10).join();
        assertEquals(1, b1.size());

        final Page<Map<String, Object>> b2 = storage.query(schemaA, Expression.parse("ref.id == 'b:2'"), ImmutableList.of(), ImmutableSet.of())
                .page(10).join();
        assertEquals(1, b2.size());
    }
}

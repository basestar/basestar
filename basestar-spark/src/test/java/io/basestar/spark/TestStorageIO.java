package io.basestar.spark;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.basestar.schema.Instance;
import io.basestar.schema.Namespace;
import io.basestar.schema.ObjectSchema;
import io.basestar.schema.util.Ref;
import io.basestar.spark.database.SparkDatabase;
import io.basestar.spark.hadoop.StorageInputFormat;
import io.basestar.spark.hadoop.StorageProvider;
import io.basestar.spark.query.QueryResolver;
import io.basestar.storage.MemoryStorage;
import io.basestar.storage.Storage;
import io.basestar.util.Name;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;

public class TestStorageIO extends AbstractSparkTest {

    private static final Namespace namespace;
    static {
        try {
            namespace = Namespace.load(SparkDatabaseTest.class.getResourceAsStream("schema.yml"));
        } catch (final IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public static class ExampleStorageProvider implements StorageProvider {

        private final MemoryStorage storage = MemoryStorage.builder()
                .build();

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
    void testStorageWrite() throws IOException {

        final SparkSession session = session();

        final Dataset<Row> datasetA = session.createDataset(ImmutableList.of(
                new AbstractSparkTest.A("a:1", new AbstractSparkTest.B("b:1"), ImmutableList.of(new AbstractSparkTest.B("b:3"), new AbstractSparkTest.B("b:4")), ImmutableMap.of("x", new AbstractSparkTest.B("b:5")), new AbstractSparkTest.E(new AbstractSparkTest.B("b:1"))),
                new AbstractSparkTest.A("a:2", new AbstractSparkTest.B("b:2"), ImmutableList.of(), ImmutableMap.of(), null)
        ), Encoders.bean(AbstractSparkTest.A.class)).toDF();

        final Map<Name, Dataset<Row>> datasets = ImmutableMap.of(
                Name.of("A"), datasetA
        );

        final ObjectSchema schemaA = namespace.requireObjectSchema("A");

        final QueryResolver resolver = new QueryResolver.Automatic(QueryResolver.source(schema -> datasets.get(schema.getQualifiedName())));

        final SparkDatabase database = SparkDatabase.builder()
                .resolver(resolver).namespace(namespace)
                .build();

        final Configuration config = new Configuration();

        config.set(StorageProvider.PROVIDER, ExampleStorageProvider.class.getName());

//        database.from("A").query().withColumn("version", functions.lit(1L)).toJavaRDD().mapToPair(row -> {
//            final Map<String, Object> after = SparkSchemaUtils.fromSpark(schemaA, row);
//            return Tuple2.apply(Ref.of(schemaA.getQualifiedName(), Instance.getId(after)), new WriteAction.Create(after));
//        }).saveAsNewAPIHadoopFile("", Ref.class, WriteAction.class, StorageOutputFormat.class, config);

        session.sparkContext().newAPIHadoopRDD(config, StorageInputFormat.class, Ref.class, Instance.class)
                .collect();
    }
}

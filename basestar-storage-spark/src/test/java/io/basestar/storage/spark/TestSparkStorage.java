package io.basestar.storage.spark;

import com.google.common.collect.Multimap;
import io.basestar.schema.Namespace;
import io.basestar.schema.ObjectSchema;
import io.basestar.spark.SparkUtils;
import io.basestar.storage.Storage;
import io.basestar.storage.TestStorage;
import io.basestar.util.Nullsafe;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class TestSparkStorage extends TestStorage {

    @Override
    protected Storage storage(final Namespace namespace, final Multimap<String, Map<String, Object>> data) {

        final SparkSession session = SparkSession.builder()
                .master("local")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .getOrCreate();

        final SparkRouting routing = new SparkRouting() {

            @Override
            public Dataset<Row> objectRead(final SparkSession session, final ObjectSchema schema) {

                final StructType structType = SparkUtils.structType(schema);
                final List<Row> items = Nullsafe.of(data.get(schema.getName())).stream()
                        .map(schema::create)
                        .map(v -> SparkUtils.toSpark(schema, structType, v))
                        .collect(Collectors.toList());

                return session.sqlContext()
                        .createDataFrame(items, structType);
            }

            @Override
            public Dataset<Row> historyRead(final SparkSession session, final ObjectSchema schema) {

                return objectRead(session, schema);
            }
        };

        return SparkStorage.builder()
                .setRouting(routing)
                .setSession(session)
                .build();
    }

    @Override
    public void testLarge() {

    }

    @Override
    public void testCreate() {

    }

    @Override
    public void testUpdate() {

    }

    @Override
    public void testDelete() {

    }
}

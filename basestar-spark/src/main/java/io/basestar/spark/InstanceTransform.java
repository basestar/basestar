package io.basestar.spark;

import io.basestar.schema.Instance;
import io.basestar.schema.InstanceSchema;
import lombok.Builder;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

@Builder
public class InstanceTransform implements Transform<Dataset<Row>, RDD<Map<String, Object>>> {

    private final InstanceSchema schema;

    @Override
    public RDD<Map<String, Object>> accept(final Dataset<Row> input) {

        return input.toJavaRDD().map(row -> {
            final Map<String, Object> instance = new HashMap<>(SparkSchemaUtils.fromSpark(schema, row));
            if(Instance.getCreated(instance) == null) {
                Instance.setCreated(instance, LocalDateTime.now());
            }
            if(Instance.getUpdated(instance) == null) {
                Instance.setUpdated(instance, LocalDateTime.now());
            }
            if(Instance.getVersion(instance) == null) {
                Instance.setVersion(instance, 1L);
            }
            return instance;
        }).rdd();
    }
}

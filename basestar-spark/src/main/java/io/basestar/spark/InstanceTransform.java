package io.basestar.spark;

import io.basestar.schema.InstanceSchema;
import lombok.Builder;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Map;

@Builder
public class InstanceTransform implements Transform<Dataset<Row>, RDD<Map<String, Object>>> {

    private final InstanceSchema schema;

    @Override
    public RDD<Map<String, Object>> accept(final Dataset<Row> input) {

        return input.toJavaRDD().map(row -> SparkSchemaUtils.fromSpark(schema, row)).rdd();
    }
}

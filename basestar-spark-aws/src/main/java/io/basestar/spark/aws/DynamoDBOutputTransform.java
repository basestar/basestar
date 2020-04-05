package io.basestar.spark.aws;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import io.basestar.schema.InstanceSchema;
import io.basestar.spark.SparkUtils;
import io.basestar.spark.Transform;
import lombok.RequiredArgsConstructor;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Map;

@RequiredArgsConstructor
public class DynamoDBOutputTransform implements Transform<Dataset<Row>, RDD<Map<String, AttributeValue>>> {

    private final InstanceSchema schema;

    @Override
    public RDD<Map<String, AttributeValue>> apply(final Dataset<Row> input) {

        return input.toJavaRDD().map(row -> {
            final Map<String, Object> data = SparkUtils.fromSpark(schema, row);
            return DynamoDBSparkUtils.toDynamoDB(data);
        }).rdd();
    }
}

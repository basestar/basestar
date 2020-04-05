package io.basestar.spark.aws;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import io.basestar.schema.InstanceSchema;
import io.basestar.spark.SparkUtils;
import io.basestar.spark.Transform;
import lombok.RequiredArgsConstructor;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.StructType;

import java.util.Map;

@RequiredArgsConstructor
public class DynamoDBInputTransform implements Transform<RDD<Map<String, AttributeValue>>, Dataset<Row>> {

    private final InstanceSchema schema;

    private final StructType structType;

    public DynamoDBInputTransform(final InstanceSchema schema) {

        this.schema = schema;
        this.structType = DynamoDBSparkUtils.structType(schema);
    }

    @Override
    public Dataset<Row> apply(final RDD<Map<String, AttributeValue>> input) {

        final SQLContext sqlContext = new SQLContext(input.sparkContext());
        return sqlContext.createDataFrame(input.toJavaRDD()
                .map(values -> {
                    final Map<String, Object> data = DynamoDBSparkUtils.fromDynamoDB(values);
                    return SparkUtils.toSpark(schema, structType, data);
                }), structType);
    }
}

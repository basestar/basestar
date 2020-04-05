package io.basestar.spark.aws;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.Record;
import io.basestar.spark.Transform;
import org.apache.spark.rdd.RDD;

import java.util.Map;

public class DynamoDBOldImageTransform implements Transform<RDD<Record>, RDD<Map<String, AttributeValue>>> {

    @Override
    public RDD<Map<String, AttributeValue>> apply(final RDD<Record> input) {

        return input.toJavaRDD().map(v -> v.getDynamodb().getOldImage()).rdd();
    }
}

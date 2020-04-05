package io.basestar.spark.aws;

import io.basestar.schema.Index;
import io.basestar.schema.Instance;
import io.basestar.schema.ObjectSchema;
import io.basestar.spark.SparkUtils;
import io.basestar.spark.Transform;
import io.basestar.storage.dynamodb.DynamoDBRouting;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.StructType;

import java.util.Map;

public class DynamoDBIndexTransform implements Transform<Dataset<Row>, Dataset<Row>> {

    private final DynamoDBRouting routing;

    private final ObjectSchema schema;

    private final Index index;

    public DynamoDBIndexTransform(final DynamoDBRouting routing, final ObjectSchema schema, final Index index) {

        this.routing = routing;
        this.schema = schema;
        this.index = index;
    }

    @Override
    public Dataset<Row> apply(final Dataset<Row> df) {

        final StructType structType = DynamoDBSparkUtils.type(routing, schema, index);

        return df.flatMap((FlatMapFunction<Row, Row>) row -> {

            final Map<String, Object> initial = SparkUtils.fromSpark(schema, row);
            final String id = Instance.getId(initial);
            final Map<Index.Key, Map<String, Object>> records = index.readValues(initial);

            return records.entrySet().stream()
                    .map(e -> DynamoDBSparkUtils.toSpark(routing, schema, index, structType, id, e.getKey(), e.getValue()))
                    .iterator();

        }, RowEncoder.apply(structType));
    }
}

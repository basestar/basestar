package io.basestar.spark.transform;

import io.basestar.schema.InstanceSchema;
import io.basestar.spark.util.SparkSchemaUtils;
import io.basestar.util.Nullsafe;
import lombok.Getter;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.StructType;

import java.util.Map;

@Getter
public class SchemaTransform implements Transform<Dataset<Row>, Dataset<Row>> {

    private final InstanceSchema schema;

    @lombok.Builder(builderClassName = "Builder")
    public SchemaTransform(final InstanceSchema schema) {

        this.schema = Nullsafe.require(schema);
    }

    @Override
    public Dataset<Row> accept(final Dataset<Row> input) {

        final InstanceSchema schema = this.schema;
        final StructType outputType = SparkSchemaUtils.structType(schema);

        return input.map((MapFunction<Row, Row>) row -> {

            final Map<String, Object> object = SparkSchemaUtils.fromSpark(schema, row);
            final Map<String, Object> instance = schema.create(object, schema.getExpand(), true);
            return SparkSchemaUtils.toSpark(schema, outputType, instance);

        }, RowEncoder.apply(outputType));
    }
}

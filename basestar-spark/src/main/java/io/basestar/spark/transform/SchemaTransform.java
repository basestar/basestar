package io.basestar.spark.transform;

import io.basestar.schema.InstanceSchema;
import io.basestar.spark.util.SparkSchemaUtils;
import io.basestar.spark.util.SparkUtils;
import io.basestar.util.Immutable;
import io.basestar.util.Name;
import io.basestar.util.Nullsafe;
import lombok.Getter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.StructType;

import java.util.Map;
import java.util.Set;

@Getter
public class SchemaTransform implements Transform<Dataset<Row>, Dataset<Row>> {

    private final InstanceSchema schema;

    private final Set<Name> expand;

    @lombok.Builder(builderClassName = "Builder")
    public SchemaTransform(final InstanceSchema schema, final Set<Name> expand) {

        this.schema = Nullsafe.require(schema);
        this.expand = Immutable.set(expand);
    }

    @Override
    public Dataset<Row> accept(final Dataset<Row> input) {

        final InstanceSchema schema = this.schema;
        final Set<Name> expand = this.expand;
        final StructType outputType = SparkSchemaUtils.structType(schema);

        return input.map(SparkUtils.map(row -> {

            final Map<String, Object> object = SparkSchemaUtils.fromSpark(schema, expand, row);
            final Map<String, Object> instance = schema.create(object, expand, true);
            return SparkSchemaUtils.toSpark(schema, expand, outputType, instance);

        }), RowEncoder.apply(outputType));
    }
}

package io.basestar.spark;

import io.basestar.schema.InstanceSchema;
import io.basestar.schema.use.Use;
import io.basestar.util.Nullsafe;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;

import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public class SchemaTransform implements Transform<Dataset<Row>, Dataset<Row>> {

    private final InstanceSchema schema;

    private final Map<String, Use<?>> extraMetadata;

    @lombok.Builder(builderClassName = "Builder")
    public SchemaTransform(final InstanceSchema schema, final Map<String, Use<?>> extraMetadata) {

        this.schema = Nullsafe.require(schema);
        this.extraMetadata = Nullsafe.option(extraMetadata);
    }

    @Override
    public Dataset<Row> accept(final Dataset<Row> input) {

        final SortedMap<String, Column> columns = new TreeMap<>();
        schema.getAllProperties().forEach((name, prop) -> columns.put(name, column(input, name, prop.getType())));
        schema.metadataSchema().forEach((name, type) -> columns.put(name, column(input, name, type)));
        extraMetadata.forEach((name, type) -> columns.put(name, column(input, name, type)));
        return input.select(columns.values().toArray(new Column[0]));
    }

    private Column column(final Dataset<Row> input, final String name, final Use<?> type) {

        final StructType schema = input.schema();

        return SparkSchemaUtils.findField(schema, name)
                .map(field -> column(input.col(field.name()), field.dataType(), type).as(name))
                .orElseGet(() -> nullColumn(type).as(name));
    }

    private Column nullColumn(final Use<?> type) {

        return functions.lit(null).cast(SparkSchemaUtils.type(type));
    }

    private Column column(final Column source, final DataType sourceDataType, final Use<?> type) {

        final DataType targetDataType = SparkSchemaUtils.type(type);
        if(targetDataType.equals(sourceDataType)) {
            return source;
        } else {
            final Use<?> sourceType = SparkSchemaUtils.type(sourceDataType);

            final UserDefinedFunction udf = functions.udf(
                    (Object sourceValue) -> {
                        final Object targetValue = type.create(SparkSchemaUtils.fromSpark(sourceType, sourceValue));
                        return SparkSchemaUtils.toSpark(type, targetDataType, targetValue);
                    },
                    targetDataType
            );
            return udf.apply(source);
        }
    }
}

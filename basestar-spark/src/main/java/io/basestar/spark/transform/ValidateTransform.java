package io.basestar.spark.transform;

import com.google.common.collect.ImmutableMap;
import io.basestar.expression.Context;
import io.basestar.schema.Constraint;
import io.basestar.schema.Instance;
import io.basestar.schema.InstanceSchema;
import io.basestar.schema.Reserved;
import io.basestar.schema.use.Use;
import io.basestar.schema.use.UseArray;
import io.basestar.schema.use.UseString;
import io.basestar.schema.use.UseStruct;
import io.basestar.spark.util.SparkSchemaUtils;
import io.basestar.spark.util.SparkUtils;
import io.basestar.util.Nullsafe;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.StructType;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class ValidateTransform implements Transform<Dataset<Row>, Dataset<Row>> {

    private final InstanceSchema schema;

    private final Map<String, Use<?>> extraMetadata = ImmutableMap.of(
            Reserved.PREFIX + "violations", new UseArray<>(UseStruct.from(ImmutableMap.of(
                    "name", UseString.DEFAULT,
                    "type", UseString.DEFAULT,
                    "message", UseString.DEFAULT
            )))
    );

    @lombok.Builder(builderClassName = "Builder")
    ValidateTransform(final InstanceSchema schema) {

        this.schema = schema;
    }

    @Override
    public Dataset<Row> accept(final Dataset<Row> input) {

        final StructType structType = SparkSchemaUtils.structType(schema, schema.getExpand(), extraMetadata);
        return input.map(SparkUtils.map(row -> {

            final Instance instance = schema.create(SparkSchemaUtils.fromSpark(schema, schema.getExpand(), row));
            final Set<Constraint.Violation> violations = schema.validate(Context.init(), instance);
            final Map<String, Object> result = new HashMap<>(instance);
            result.put(Reserved.PREFIX + "violations", violations.stream()
                    .map(v -> ImmutableMap.of(
                            "name", v.getName().toString(),
                            "type", v.getType(),
                            "message", Nullsafe.orDefault(v.getMessage())
                    ))
                    .collect(Collectors.toList()));
            return SparkSchemaUtils.toSpark(schema, schema.getExpand(), extraMetadata, structType, result);

        }), RowEncoder.apply(structType));
    }
}

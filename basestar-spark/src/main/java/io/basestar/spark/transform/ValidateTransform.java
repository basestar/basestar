package io.basestar.spark.transform;

import com.google.common.collect.ImmutableMap;
import io.basestar.expression.Context;
import io.basestar.schema.Constraint;
import io.basestar.schema.Instance;
import io.basestar.schema.InstanceSchema;
import io.basestar.schema.Reserved;
import io.basestar.schema.use.*;
import io.basestar.spark.util.SparkSchemaUtils;
import io.basestar.spark.util.SparkUtils;
import io.basestar.util.Immutable;
import io.basestar.util.Nullsafe;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.StructType;

import java.util.*;

public class ValidateTransform implements Transform<Dataset<Row>, Dataset<Row>> {

    public static final Map<String, Use<?>> VIOLATIONS_METADATA = ImmutableMap.of(
            Reserved.PREFIX + "violations", new UseMap<>(new UseArray<>(UseStruct.from(ImmutableMap.of(
                    "name", UseString.DEFAULT,
                    "type", UseString.DEFAULT,
                    "message", UseString.DEFAULT
            ))))
    );

    private final InstanceSchema schema;

    private final Map<String, Use<?>> extraMetadata;

    @lombok.Builder(builderClassName = "Builder")
    ValidateTransform(final InstanceSchema schema, final Map<String, Use<?>> extraMetadata) {

        this.schema = schema;
        this.extraMetadata = Immutable.map(extraMetadata);
    }

    @Override
    public Dataset<Row> accept(final Dataset<Row> input) {

        final Map<String, Use<?>> metadata = new HashMap<>(extraMetadata);
        metadata.putAll(VIOLATIONS_METADATA);
        final InstanceSchema schema = this.schema;
        final StructType structType = SparkSchemaUtils.structType(schema, schema.getExpand(), metadata);
        return input.map(SparkUtils.map(row -> {

            final Instance instance = schema.create(SparkSchemaUtils.fromSpark(schema, schema.getExpand(), metadata, row));
            final Set<Constraint.Violation> violations = schema.validate(Context.init(), instance);
            final Map<String, Object> result = new HashMap<>(instance);
            final Map<String, List<Map<String, Object>>> output = new HashMap<>();
            violations.forEach(v -> v.getEffectiveGroups().forEach(g -> {
                output.computeIfAbsent(g, ignored -> new ArrayList<>()).add(ImmutableMap.of(
                        "name", v.getName().toString(),
                        "type", v.getType(),
                        "message", Nullsafe.orDefault(v.getMessage())
                ));
            }));
            return SparkSchemaUtils.toSpark(schema, schema.getExpand(), metadata, structType, result);

        }), RowEncoder.apply(structType));
    }
}

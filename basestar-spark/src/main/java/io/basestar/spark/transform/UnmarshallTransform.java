package io.basestar.spark.transform;

import com.google.common.collect.ImmutableSet;
import io.basestar.mapper.MappingContext;
import io.basestar.mapper.internal.InstanceSchemaMapper;
import io.basestar.schema.InstanceSchema;
import io.basestar.schema.Namespace;
import io.basestar.spark.util.SparkSchemaUtils;
import lombok.RequiredArgsConstructor;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.StructType;

import java.util.Map;

@RequiredArgsConstructor
public class UnmarshallTransform<T> implements Transform<Dataset<T>, Dataset<Row>> {

    private final InstanceSchemaMapper<T, ?> mapper;

    private final InstanceSchema schema;

    @SuppressWarnings({"unchecked", "rawtypes"})
    public UnmarshallTransform(final MappingContext context, final Class<T> cls) {

        final Namespace namespace = context.namespace(cls).build();
        this.schema = namespace.requireInstanceSchema(context.schemaName(cls));
        // FIXME: should provide an instanceSchemaMapper method in MappingContext so we don't have to cast like this
        this.mapper = (InstanceSchemaMapper<T, ?>)(InstanceSchemaMapper)context.schemaMapper(cls);
    }

    public UnmarshallTransform(final Class<T> cls) {

        this(new MappingContext(), cls);
    }

    @Override
    public Dataset<Row> accept(final Dataset<T> input) {

        final StructType structType = SparkSchemaUtils.structType(schema, ImmutableSet.of());
        return input.map((MapFunction<T, Row>) object -> {
            final Map<String, Object> data = mapper.unmarshall(object);
            return SparkSchemaUtils.toSpark(schema, structType, data);
        }, RowEncoder.apply(structType));
    }
}

package io.basestar.spark.transform;

import io.basestar.mapper.MappingContext;
import io.basestar.mapper.SchemaMapper;
import io.basestar.schema.InstanceSchema;
import io.basestar.schema.Namespace;
import io.basestar.spark.util.SparkSchemaUtils;
import io.basestar.util.Nullsafe;
import lombok.RequiredArgsConstructor;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;

import java.util.Map;

@RequiredArgsConstructor
public class MarshallTransform<T> implements Transform<Dataset<Row>, Dataset<T>> {

    private final SchemaMapper<T, ? extends Map<String, Object>> mapper;

    private final InstanceSchema schema;

    @lombok.Builder(builderClassName = "Builder")
    MarshallTransform(final MappingContext context, final Class<T> targetType) {

        Nullsafe.require(targetType);
        final MappingContext resolvedContext = Nullsafe.orDefault(context, MappingContext::new);
        final Namespace namespace = resolvedContext.namespace(targetType).build();
        this.schema = namespace.requireInstanceSchema(resolvedContext.schemaName(targetType));
        this.mapper = resolvedContext.schemaMapper(targetType);
    }

    @Override
    public Dataset<T> accept(final Dataset<Row> input) {

        final SchemaMapper<T, ? extends Map<String, Object>> mapper = this.mapper;
        final InstanceSchema schema = this.schema;
        final Class<T> beanClass = mapper.marshalledType();
        return input.map((MapFunction<Row, T>) row -> {
            final Map<String, Object> object = SparkSchemaUtils.fromSpark(schema, row);
            return mapper.marshall(object);
        }, Encoders.bean(beanClass));
    }
}

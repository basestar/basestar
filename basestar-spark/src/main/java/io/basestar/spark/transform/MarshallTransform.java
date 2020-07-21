package io.basestar.spark.transform;

import io.basestar.mapper.MappingContext;
import io.basestar.mapper.SchemaMapper;
import io.basestar.schema.InstanceSchema;
import io.basestar.schema.Namespace;
import io.basestar.spark.util.SparkSchemaUtils;
import lombok.RequiredArgsConstructor;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;

import java.util.Map;

@RequiredArgsConstructor
public class MarshallTransform<T> implements Transform<Dataset<?>, Dataset<T>> {

    private final SchemaMapper<T, ? extends Map<String, Object>> mapper;

    private final InstanceSchema schema;

    public MarshallTransform(final MappingContext context, final Class<T> cls) {

        final Namespace namespace = context.namespace(cls).build();
        this.schema = namespace.requireInstanceSchema(context.schemaName(cls));
        this.mapper = context.schemaMapper(cls);
    }

    public MarshallTransform(final Class<T> cls) {

        this(new MappingContext(), cls);
    }

    @Override
    public Dataset<T> accept(final Dataset<?> input) {

        final Class<T> beanClass = mapper.marshalledType();
        return input.toDF().map((MapFunction<Row, T>) row -> {
            final Map<String, Object> object = SparkSchemaUtils.fromSpark(schema, row);
            return mapper.marshall(object);
        }, Encoders.bean(beanClass));
    }
}

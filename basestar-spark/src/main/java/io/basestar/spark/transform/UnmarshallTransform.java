package io.basestar.spark.transform;

import com.google.common.collect.ImmutableSet;
import io.basestar.mapper.MappingContext;
import io.basestar.mapper.internal.InstanceSchemaMapper;
import io.basestar.schema.InstanceSchema;
import io.basestar.schema.LinkableSchema;
import io.basestar.schema.Namespace;
import io.basestar.spark.util.SparkSchemaUtils;
import io.basestar.util.Name;
import io.basestar.util.Nullsafe;
import lombok.RequiredArgsConstructor;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.StructType;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

@RequiredArgsConstructor
public class UnmarshallTransform<T> implements Transform<Dataset<T>, Dataset<Row>> {

    private final InstanceSchemaMapper<T, ?> mapper;

    private final InstanceSchema schema;

    private final Set<Name> expand;

    @SuppressWarnings({"unchecked", "rawtypes"})
    public UnmarshallTransform(final MappingContext context, final Class<T> sourceType) {

        Nullsafe.require(sourceType);
        final MappingContext resolvedContext = Nullsafe.option(context, MappingContext::new);
        final Namespace namespace = resolvedContext.namespace(sourceType).build();
        this.schema = namespace.requireInstanceSchema(resolvedContext.schemaName(sourceType));
        if(this.schema instanceof LinkableSchema) {
            this.expand = ((LinkableSchema) this.schema).getExpand();
        } else {
            this.expand = Collections.emptySet();
        }
        // FIXME: should provide an instanceSchemaMapper method in MappingContext so we don't have to cast like this
        this.mapper = (InstanceSchemaMapper<T, ?>)(InstanceSchemaMapper)resolvedContext.schemaMapper(sourceType);
    }

    @Override
    public Dataset<Row> accept(final Dataset<T> input) {

        final InstanceSchemaMapper<T, ?> mapper = this.mapper;
        final InstanceSchema schema = this.schema;
        final Set<Name> expand = this.expand;
        final StructType structType = SparkSchemaUtils.structType(schema, ImmutableSet.of());
        return input.map((MapFunction<T, Row>) object -> {
            final Map<String, Object> data = mapper.unmarshall(object);
            return SparkSchemaUtils.toSpark(schema, expand, structType, data);
        }, RowEncoder.apply(structType));
    }
}

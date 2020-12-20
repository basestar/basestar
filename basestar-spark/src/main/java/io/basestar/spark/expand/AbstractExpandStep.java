package io.basestar.spark.expand;

import com.google.common.collect.Lists;
import io.basestar.schema.InstanceSchema;
import io.basestar.schema.Link;
import io.basestar.schema.LinkableSchema;
import io.basestar.schema.Member;
import io.basestar.schema.use.Use;
import io.basestar.schema.use.UseCollection;
import io.basestar.schema.use.UseInstance;
import io.basestar.schema.use.UseMap;
import io.basestar.spark.query.QueryResolver;
import io.basestar.spark.util.SparkRowUtils;
import io.basestar.spark.util.SparkSchemaUtils;
import io.basestar.util.Name;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.KeyValueGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.*;
import scala.Tuple2;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
public abstract class AbstractExpandStep implements ExpandStep {

    @Override
    public Dataset<Row> apply(final QueryResolver resolver, final Dataset<Row> input) {

        log.warn(describe());
        final Dataset<Row> output;
        if (hasProjectedKeys(input.schema())) {
            output = applyImpl(resolver, input, getRoot().typeOfId());
        } else {
            final StructType inputType = input.schema();
            final StructType projectedType = projectKeysType(inputType);
            output = applyImpl(resolver, input.flatMap(
                    (FlatMapFunction<Row, Row>) row -> projectKeys(projectedType, row),
                    RowEncoder.apply(projectedType)
            ), getRoot().typeOfId());
        }
        return output;
    }

    protected abstract LinkableSchema getRoot();

    protected abstract String describe();

    protected abstract boolean hasProjectedKeys(StructType schema);

    protected abstract <T> Dataset<Row> applyImpl(QueryResolver resolver, Dataset<Row> input, Use<T> typeOfId);

    @SuppressWarnings("unchecked")
    protected <T> KeyValueGroupedDataset<T, Tuple2<Row, Row>> groupResults(final Dataset<Tuple2<Row, Row>> input) {

        final LinkableSchema root = getRoot();

        final Encoder<T> keyEncoder = (Encoder<T>) SparkSchemaUtils.encoder(root.typeOfId());
        return input
                .groupByKey(
                        (MapFunction<Tuple2<Row, Row>, T>) (tuple -> (T)SparkRowUtils.get(tuple._1(), root.id())),
                        keyEncoder
                );
    }

    protected static StructType expandedType(final InstanceSchema inputSchema, final Set<Name> names, final StructType inputType, final DataType joinToType) {

        final List<StructField> fields = Lists.newArrayList(inputType.fields());
        final Map<String, Set<Name>> branches = Name.branch(names);
        branches.forEach((name, expand) -> {
            final Member member = inputSchema.requireMember(name, true);
            final int i = inputType.fieldIndex(name);
            final StructField field = fields.get(i);
            assert field != null;
            if(expand.isEmpty() && member instanceof Link) {
                fields.set(i, SparkRowUtils.field(name, joinToType));
            } else {
                fields.set(i, SparkRowUtils.field(name, expandedType(member.getType(), expand, field.dataType(), joinToType)));
            }
        });
        return DataTypes.createStructType(fields).asNullable();
    }

    protected static DataType expandedType(final Use<?> type, final Set<Name> expand, final DataType inputType, final DataType joinToType) {

        return type.visit(new Use.Visitor.Defaulting<DataType>() {

            @Override
            public <V, T extends Collection<V>> DataType visitCollection(final UseCollection<V, T> type) {

                if (inputType instanceof ArrayType) {
                    return DataTypes.createArrayType(expandedType(type.getType(), expand, ((ArrayType) inputType).elementType(), joinToType));
                } else {
                    throw new IllegalStateException();
                }
            }

            @Override
            public <T> DataType visitMap(final UseMap<T> type) {

                if (inputType instanceof MapType) {
                    final MapType mapType = (MapType)inputType;
                    final Map<String, Set<Name>> branches = Name.branch(expand);
                    final Set<Name> branch = branches.get(UseMap.EXPAND_WILDCARD);
                    if (branch != null) {
                        return DataTypes.createMapType(mapType.keyType(), expandedType(type.getType(), branch, mapType.valueType(), joinToType));
                    } else {
                        throw new IllegalStateException();
                    }
                } else {
                    throw new IllegalStateException();
                }
            }

            @Override
            public DataType visitInstance(final UseInstance type) {

                if (inputType instanceof StructType) {
                    if (expand.isEmpty()) {
                        assert joinToType instanceof StructType;
                        return joinToType;
                    } else {
                        return expandedType(type.getSchema(), expand, (StructType)inputType, joinToType);
                    }
                } else {
                    throw new IllegalStateException();
                }
            }
        });
    }
}

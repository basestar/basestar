package io.basestar.spark.transform;

import com.google.common.collect.ImmutableMap;
import io.basestar.schema.Index;
import io.basestar.schema.Instance;
import io.basestar.schema.ObjectSchema;
import io.basestar.schema.Reserved;
import io.basestar.schema.use.Use;
import io.basestar.schema.use.UseBoolean;
import io.basestar.spark.util.SparkSchemaUtils;
import io.basestar.util.Immutable;
import io.basestar.util.Nullsafe;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.Map;
import java.util.stream.Stream;

public class IndexDeltaTransform implements Transform<Dataset<Tuple2<Row, Row>>, Dataset<Row>> {

    private final ObjectSchema schema;

    private final Index index;

    @lombok.Builder(builderClassName = "Builder")
    IndexDeltaTransform(final ObjectSchema schema, final Index index) {

        this.schema = Nullsafe.require(schema);
        this.index = Nullsafe.require(index);
    }

    @Override
    public Dataset<Row> accept(final Dataset<Tuple2<Row, Row>> input) {

        final Map<String, Use<?>> extraMetadata = ImmutableMap.of(
                Reserved.DELETED, UseBoolean.DEFAULT
        );

        final ObjectSchema schema = this.schema;
        final Index index = this.index;

        final StructType structType = SparkSchemaUtils.structType(schema, index, extraMetadata);
        return input.flatMap((FlatMapFunction<Tuple2<Row, Row>, Row>)(pair -> {

            final Map<String, Object> before = SparkSchemaUtils.fromSpark(schema, schema.getExpand(), pair._1());
            final Map<String, Object> after = SparkSchemaUtils.fromSpark(schema, schema.getExpand(), pair._2());
            final Index.Diff diff = index.diff(before, after);

            final Stream<Row> upsert = Stream.of(diff.getCreate(), diff.getUpdate())
                    .flatMap(source -> source.entrySet().stream())
                    .map(entry -> {
                        final Map<String, Object> data = Immutable.copyPut(entry.getValue(), Reserved.DELETED, false);
                        return SparkSchemaUtils.toSpark(schema, index, extraMetadata, structType, entry.getKey().binary(), data);
                    });

            final Stream<Row> delete = diff.getDelete().stream()
                    .map(key -> {
                        final Map<String, Object> data = ImmutableMap.of(
                                ObjectSchema.ID, Instance.getId(before),
                                Reserved.DELETED, true
                        );
                        return SparkSchemaUtils.toSpark(schema, index, extraMetadata, structType, key.binary(), data);
                    });

            return Stream.concat(upsert, delete).iterator();

        }), RowEncoder.apply(structType));
    }
}

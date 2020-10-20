//package io.basestar.spark.transform;
//
//import com.google.common.base.MoreObjects;
//import io.basestar.schema.InstanceSchema;
//import io.basestar.schema.use.Use;
//import io.basestar.spark.util.SparkSchemaUtils;
//import lombok.RequiredArgsConstructor;
//import org.apache.spark.api.java.function.FlatMapFunction;
//import org.apache.spark.sql.Column;
//import org.apache.spark.sql.Dataset;
//import org.apache.spark.sql.Row;
//import org.apache.spark.sql.catalyst.encoders.RowEncoder;
//import org.apache.spark.sql.types.StructType;
//import scala.Serializable;
//import scala.Tuple2;
//
//import java.util.Collections;
//import java.util.Map;
//
//@RequiredArgsConstructor
//public class CombiningTransform implements Transform<Dataset<Row>, Dataset<Row>> {
//
//    private final Dataset<Row> overlay;
//
//    private final Combiner combiner;
//
//    @Override
//    public Dataset<Row> accept(final Dataset<Row> input) {
//
//        final StructType structType = combiner.outputType();
//        final Column condition = combiner.condition(input, overlay);
//        return input.joinWith(overlay, condition, combiner.joinType())
//                .flatMap((FlatMapFunction<Tuple2<Row, Row>, Row>) v -> {
//
//                    final Row result = combiner.combine(structType, v._1(), v._2());
//                    if(result == null) {
//                        return Collections.emptyIterator();
//                    } else {
//                        return Collections.singleton(result).iterator();
//                    }
//
//                }, RowEncoder.apply(structType));
//    }
//
//    public interface Combiner extends Serializable {
//
//        default String joinType() {
//
//            return "full_outer";
//        }
//
//        StructType outputType();
//
//        Column condition(Dataset<Row> baseline, Dataset<Row> overlay);
//
//        Row combine(StructType structType, Row baseline, Row overlay);
//
//        Map<String, Object> combine(InstanceSchema schema, Map<String, Object> baseline, Map<String, Object> overlay);
//    }
//
//    public interface InstanceCombiner extends Combiner {
//
//        InstanceSchema schema();
//
//        Map<String, Object> combine(InstanceSchema schema, Map<String, Object> baseline, Map<String, Object> overlay);
//
//        default Map<String, Use<?>> extraMetadataSchema(final InstanceSchema schema) {
//
//            return Collections.emptyMap();
//        }
//
//        @Override
//        default StructType outputType() {
//
//            final InstanceSchema schema = schema();
//            return SparkSchemaUtils.structType(schema, schema.getExpand(), extraMetadataSchema(schema));
//        }
//
//        default Column condition(final Dataset<Row> baseline, final Dataset<Row> overlay) {
//
//            final InstanceSchema schema = schema();
//            return baseline.col(schema.id()).equalTo(overlay.col(schema.id()));
//        }
//
//        default Row combine(final StructType structType, final Row baseline, final Row overlay) {
//
//            final InstanceSchema schema = schema();
//            final Map<String, Object> baselineInstance = SparkSchemaUtils.fromSpark(schema, schema.getExpand(), baseline);
//            final Map<String, Object> overlayInstance = SparkSchemaUtils.fromSpark(schema, schema.getExpand(), overlay);
//            final Map<String, Object> resultInstance = combine(schema, baselineInstance, overlayInstance);
//            return SparkSchemaUtils.toSpark(schema, schema.getExpand(), structType, resultInstance);
//        }
//    }
//
//    @RequiredArgsConstructor
//    public static class OverlayCombiner implements InstanceCombiner {
//
//        private final InstanceSchema schema;
//
//        @Override
//        public InstanceSchema schema() {
//
//            return schema;
//        }
//
//        @Override
//        public Map<String, Object> combine(final InstanceSchema schema, final Map<String, Object> baseline, final Map<String, Object> overlay) {
//
//            return MoreObjects.firstNonNull(baseline, overlay);
//        }
//    }
//}

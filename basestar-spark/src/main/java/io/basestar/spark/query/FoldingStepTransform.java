package io.basestar.spark.query;

import com.google.common.collect.Iterators;
import com.google.common.collect.Streams;
import io.basestar.schema.Layout;
import io.basestar.spark.transform.Transform;
import io.basestar.spark.util.SparkSchemaUtils;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Map;

public interface FoldingStepTransform extends Transform<Dataset<Row>, Dataset<Row>> {

    Layout getInputLayout();

    Layout getOutputLayout();

    Arity getArity();

    enum Arity {

        MAPPING,
        FILTERING,
        FLAT_MAPPING
    }

    Step step();

    interface Step extends Serializable {

        Iterator<? extends Map<String, Object>> apply(Map<String, Object> input);
    }

    @Override
    default Dataset<Row> accept(final Dataset<Row> input) {

        final Logger log = LoggerFactory.getLogger(getClass());

        final Arity arity = getArity();
        final Layout outputLayout = getOutputLayout();
        final Layout inputLayout = getInputLayout();
        if(arity == Arity.FLAT_MAPPING) {

            final Step step = step();
            final StructType outputStructType = SparkSchemaUtils.structType(outputLayout);
            final Dataset<Row> output = input.flatMap((FlatMapFunction<Row, Row>) row -> {

                final Map<String, Object> source = SparkSchemaUtils.fromSpark(inputLayout, row);
                final Iterator<? extends Map<String, Object>> result = step.apply(source);
                return Iterators.transform(result, v -> SparkSchemaUtils.toSpark(outputLayout, outputStructType, v));

            }, RowEncoder.apply(outputStructType));

            log.warn("Flat mapping folded step has i/o partitions {}->{}", input.rdd().partitions().length, output.rdd().partitions().length);

            return output;

        } else if(arity == Arity.FILTERING) {

            final Step step = step();
            final Dataset<Row> output = input.filter((FilterFunction<Row>) row -> {
                final Map<String, Object> source = SparkSchemaUtils.fromSpark(inputLayout, row);
                final Iterator<? extends Map<String, Object>> result = step.apply(source);
                return result.hasNext();
            });

            log.warn("Filtering folded step has i/o partitions {}->{}", input.rdd().partitions().length, output.rdd().partitions().length);

            return output;
        } else if(arity == Arity.MAPPING) {

            final Step step = step();
            final StructType outputStructType = SparkSchemaUtils.structType(outputLayout);
            final Dataset<Row> output = input.map((MapFunction<Row, Row>) row -> {

                final Map<String, Object> source = SparkSchemaUtils.fromSpark(inputLayout, row);
                final Iterator<? extends Map<String, Object>> result = step.apply(source);
                // Should be a one-to-one step, so should not return empty iterator
                assert result.hasNext();
                return SparkSchemaUtils.toSpark(outputLayout, outputStructType, result.next());

            }, RowEncoder.apply(outputStructType));

            log.warn("Mapping folded step has i/o partitions {}->{}", input.rdd().partitions().length, output.rdd().partitions().length);

            return output;
        } else {
            throw new IllegalStateException("Mergeable transform must be mapping or filtering");
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    default <O2> Transform<Dataset<Row>, O2> then(final Transform<Dataset<Row>, O2> next) {

        if(next instanceof FoldingStepTransform) {
            final FoldingStepTransform parent = this;
            final FoldingStepTransform merge = (FoldingStepTransform)next;
            return (Transform<Dataset<Row>, O2>)new FoldingStepTransform() {

                @Override
                public Layout getInputLayout() {

                    return parent.getInputLayout();
                }

                @Override
                public Layout getOutputLayout() {

                    return merge.getOutputLayout();
                }

                @Override
                public Arity getArity() {

                    final Arity parentArity = parent.getArity();
                    final Arity mergeArity = merge.getArity();
                    final boolean flatMapping = parentArity == Arity.FLAT_MAPPING || mergeArity == Arity.FLAT_MAPPING;
                    final boolean mapping = parentArity == Arity.MAPPING || mergeArity == Arity.MAPPING;
                    final boolean filtering = parentArity == Arity.FILTERING || mergeArity == Arity.FILTERING;
                    if(flatMapping || (mapping && filtering)) {
                        return Arity.FLAT_MAPPING;
                    } else if(mapping) {
                        return Arity.MAPPING;
                    } else {
                        assert filtering;
                        return Arity.FILTERING;
                    }
                }

                @Override
                @SuppressWarnings({"UnstableApiUsage"})
                public Step step() {

                    final Step parentStep = parent.step();
                    final Step mergeStep = merge.step();
                    return (input) -> {
                        final Iterator<? extends Map<String, Object>> parentIter = parentStep.apply(input);
                        return Streams.stream(parentIter).flatMap(v -> Streams.stream(mergeStep.apply(v))).iterator();
                    };
                }
            };
        } else {
            return Transform.super.then(next);
        }
    }
}

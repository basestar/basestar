package io.basestar.spark.query;

import io.basestar.spark.resolver.ColumnResolver;
import io.basestar.spark.transform.Transform;
import io.basestar.spark.util.SparkRowUtils;
import io.basestar.util.Sort;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.List;

@Slf4j
@lombok.Builder(builderClassName = "Builder")
public class SortStepTransform implements Transform<Dataset<Row>, Dataset<Row>> {

    private final List<Sort> sort;

    @Override
    public Dataset<Row> accept(final Dataset<Row> input) {

        final ColumnResolver<Row> columnResolver = ColumnResolver.lowercase(ColumnResolver::nested);

        final int inputPartitions = input.rdd().partitions().length;

        final Column[] sortExprs = sort.stream()
                .map(v -> SparkRowUtils.order(columnResolver.resolve(input, v.getName()), v.getOrder(), v.getNulls()))
                .toArray(Column[]::new);

        final Dataset<Row> output = input.sort(sortExprs);

        log.warn("Sort by ({}) has i/o partitions {}->{}", sort, inputPartitions, output.rdd().partitions().length);

        return output;
    }
}

package io.basestar.spark.query;

import io.basestar.spark.resolver.ColumnResolver;
import io.basestar.spark.transform.Transform;
import io.basestar.spark.util.SparkSchemaUtils;
import io.basestar.util.Sort;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.List;

@lombok.Builder(builderClassName = "Builder")
public class SortStepTransform implements Transform<Dataset<Row>, Dataset<Row>> {

    private final List<Sort> sort;

    @Override
    public Dataset<Row> accept(final Dataset<Row> input) {

        final ColumnResolver<Row> columnResolver = ColumnResolver.lowercase(ColumnResolver::nested);

        final Column[] sortExprs = sort.stream()
                .map(v -> SparkSchemaUtils.order(columnResolver.resolve(input, v.getName()), v.getOrder(), v.getNulls()))
                .toArray(Column[]::new);

        return input.sort(sortExprs);
    }
}

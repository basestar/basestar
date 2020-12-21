package io.basestar.spark.transform;

import io.basestar.spark.resolver.ColumnResolver;
import io.basestar.spark.util.SparkRowUtils;
import io.basestar.util.Sort;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;

import java.util.List;

@Slf4j
@lombok.Builder(builderClassName = "Builder")
public class SortTransform<T> implements Transform<Dataset<T>, Dataset<T>> {

    private final List<Sort> sort;

    @Override
    public Dataset<T> accept(final Dataset<T> input) {

        final ColumnResolver<T> columnResolver = ColumnResolver.lowercase(ColumnResolver::nested);

        final Column[] sortExprs = sort.stream()
                .map(v -> SparkRowUtils.order(columnResolver.resolve(input, v.getName()), v.getOrder(), v.getNulls()))
                .toArray(Column[]::new);

        return input.sort(sortExprs);
    }
}

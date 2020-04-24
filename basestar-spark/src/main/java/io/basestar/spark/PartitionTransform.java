package io.basestar.spark;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.List;

@lombok.Builder(builderClassName = "Builder")
public class PartitionTransform implements Transform<Dataset<Row>, Dataset<Row>> {

    private final List<String> partitionColumns;

    @Override
    public Dataset<Row> accept(final Dataset<Row> input) {

        return input.repartition(partitionColumns.stream().map(input::col).toArray(Column[]::new));
    }
}

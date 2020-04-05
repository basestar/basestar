package io.basestar.spark;

import io.basestar.schema.ObjectSchema;
import io.basestar.schema.Reserved;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.functions;

import java.util.stream.Stream;

public class LatestTransform implements Transform<Dataset<Row>, Dataset<Row>> {

    private final ObjectSchema schema;

    public LatestTransform(final ObjectSchema schema) {

        this.schema = schema;
    }

    @Override
    public Dataset<Row> apply(final Dataset<Row> df) {

        final WindowSpec window = Window.partitionBy(Reserved.SCHEMA, Reserved.ID);

        final Stream<String> names = Stream.concat(
                schema.metadataSchema().keySet().stream(),
                schema.getAllProperties().keySet().stream()
        );
        final Column[] cols = names
                .map(name -> functions.last(df.col(name)).over(window).as(name))
                .toArray(Column[]::new);

        return df.select(cols).dropDuplicates(Reserved.SCHEMA, Reserved.ID);
    }
}

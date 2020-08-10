package io.basestar.spark.transform;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;

public interface DatasetMapTransform extends Transform<Dataset<Row>, Dataset<Row>> {

    @Override
    default Dataset<Row> accept(final Dataset<Row> input) {

        final RowTransform rowTransform = rowTransform();
        final Encoder<Row> encoder = RowEncoder.apply(rowTransform.schema(input.schema()));
        return input.map((MapFunction<Row, Row>) rowTransform::accept, encoder);
    }

    default DatasetMapTransform then(final DatasetMapTransform next) {

        return () -> DatasetMapTransform.this.rowTransform().then(next.rowTransform());
    }

    RowTransform rowTransform();
}

package io.basestar.spark.transform;

import io.basestar.schema.LinkableSchema;
import io.basestar.spark.combiner.Combiner;
import io.basestar.util.Name;
import io.basestar.util.Nullsafe;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import scala.Tuple2;

import java.util.Set;

public class CombinerTransform implements Transform<Dataset<Tuple2<Row, Row>>, Dataset<Row>> {

    private final LinkableSchema schema;

    private final Set<Name> expand;

    private final Combiner combiner;


    @lombok.Builder(builderClassName = "Builder")
    CombinerTransform(final LinkableSchema schema, final Set<Name> expand, final Combiner combiner) {

        this.schema = Nullsafe.require(schema);
        this.expand = Nullsafe.orDefault(expand, schema.getExpand());
        this.combiner = Nullsafe.orDefault(combiner, Combiner.SIMPLE);
    }

    @Override
    public Dataset<Row> accept(final Dataset<Tuple2<Row, Row>> input) {

        return combiner.apply(schema, expand, input);
    }
}

package io.basestar.spark.transform;

import io.basestar.schema.LinkableSchema;
import io.basestar.spark.expand.ExpandStep;
import io.basestar.spark.expand.Expansion;
import io.basestar.spark.query.QueryResolver;
import io.basestar.util.Immutable;
import io.basestar.util.Name;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Set;

public class ExpandTransform implements Transform<Dataset<Row>, Dataset<Row>> {

    private final LinkableSchema schema;

    private final QueryResolver resolver;

    private final Set<Name> expand;

    @lombok.Builder(builderClassName = "Builder")
    protected ExpandTransform(final LinkableSchema schema, final QueryResolver resolver, final Set<Name> expand) {

        this.schema = schema;
        this.resolver = resolver;
        this.expand = Immutable.copy(expand);
    }

    @Override
    public Dataset<Row> accept(final Dataset<Row> input) {

        final Set<Expansion> expansion = Expansion.expansion(schema, expand);
        final ExpandStep step = ExpandStep.from(schema, expansion);
        assert step != null;
        return step.apply(resolver, input);
    }
}
